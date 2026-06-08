// Indelible Indexer — g-264 Step 2. Generic BSV infrastructure (destined for
// relay-federation; ZERO Indelible product code). Runs CO-LOCATED with the SV
// node (loopback RPC + ZMQ).
//
// What it does:
//  - ZMQ ingest of the node's firehose: hashblock (confirmed) + rawtx (mempool).
//    Builds a live forward UTXO index for WATCHED addresses (selective/per-user)
//    with spend-tracking (an input spending a tracked UTXO removes it).
//  - GET /unspent/:addr  -> serves that address's UTXOs. If we already have a
//    live index for it, validate those via gettxout and serve. If not (first
//    sight / historical backfill), enumerate candidates from a disposable
//    external hint and validate EACH through the node's gettxout (Step-1), while
//    watching the address so future activity is captured sovereignly.
//  - POST /validate {outpoints} -> gettxout filter (bridge-home Step-1 compat).
//  - GET /health -> node + index stats.
//
// The node's UTXO set (gettxout) is always the truth; the external hint is only
// a throwaway enumeration aid for addresses with no local history yet.

import { createServer } from 'node:http'
import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Subscriber } from 'zeromq'
import { merkleBranch, computeMerkleRoot } from './merkle.mjs'
import { initProofStore, isPending, addPendingIntent, saveProof, loadProof, markOrphaned } from './proofStore.mjs'

const __dirname = dirname(fileURLToPath(import.meta.url))
const DATADIR = process.env.BSV_DATADIR || '/data/bitcoin'
const PORT = Number(process.env.PORT || 9201)
// Optional cold-address enumeration hint — OFF by default (no third party baked in).
// Set GP_HINT to an https:// source you trust to enumerate UTXOs for addresses with no
// local history yet; every candidate it returns is still re-validated via the node's
// gettxout, so it is never an authority. Unset = the indexer never calls anyone.
let HINT = process.env.GP_HINT || null
if (HINT) {
  try { if (new URL(HINT).protocol !== 'https:') throw new Error('not https') }
  catch { console.warn('[indexer] ignoring invalid/non-https GP_HINT; cold-address enumeration disabled'); HINT = null }
}
// Dashboard (zero-dep HTML served at GET /). Loaded once at boot; missing file = no dashboard, API unaffected.
let DASHBOARD = ''
try { DASHBOARD = readFileSync(join(__dirname, 'dashboard.html'), 'utf8') } catch { /* dashboard optional */ }

// ---- node RPC ----
const conf = {}
for (const line of readFileSync(`${DATADIR}/bitcoin.conf`, 'utf8').split(/\r?\n/)) {
  const t = line.trim(); if (!t || t.startsWith('#')) continue
  const i = t.indexOf('='); if (i < 0) continue
  conf[t.slice(0, i).trim().toLowerCase()] = t.slice(i + 1).trim()
}
const RPC_URL = `http://127.0.0.1:${conf.rpcport || 8332}/`
const RPC_AUTH = 'Basic ' + Buffer.from(`${conf.rpcuser}:${conf.rpcpassword}`).toString('base64')
async function rpc (method, params = []) {
  const r = await fetch(RPC_URL, { method: 'POST', headers: { Authorization: RPC_AUTH, 'Content-Type': 'text/plain' }, body: JSON.stringify({ jsonrpc: '1.0', id: 'idx', method, params }) })
  const j = await r.json(); if (j.error) throw new Error(j.error.message); return j.result
}
async function gettxout (txid, vout) { try { return await rpc('gettxout', [txid, vout]) } catch { return null } }

// ---- in-memory forward index (lab; persistence is a follow-up) ----
const watched = new Set()                 // addresses being tracked
const utxos = new Map()                   // address -> Map("txid:vout" -> {value,height})
const track = (a) => { if (!utxos.has(a)) utxos.set(a, new Map()) }
const addUtxo = (a, txid, vout, value, height) => { track(a); utxos.get(a).set(`${txid}:${vout}`, { value, height }) }
const spend = (txid, vout) => { const k = `${txid}:${vout}`; for (const m of utxos.values()) if (m.delete(k)) return }
const stats = { blocks: 0, blockTxs: 0, memTxs: 0, lastBlock: null, started: Date.now() }

// seen-txid index for the g-189 share chain-check (WoC swap): a rolling window of
// recently-CONFIRMED txids the node already streams via ZMQ. Memory bounded by the block
// window. We can only authoritatively say EXISTS (mempool or recent block); a miss means
// "unknown / out-of-window", NOT "does not exist" — the caller falls back to WoC.
const SEEN_WINDOW_BLOCKS = 144            // ~24h of blocks; recently-confirmed shares resolve here
const seenTxids = new Map()               // txid -> confirmed height
const pruneSeen = (tip) => { const floor = tip - SEEN_WINDOW_BLOCKS; for (const [t, h] of seenTxids) if (h < floor) seenTxids.delete(t) }

// apply a decoded tx (block or mempool) to the index for watched addresses
function applyTx (tx, height) {
  for (const vin of tx.vin || []) if (vin.txid) spend(vin.txid, vin.vout)
  for (const vout of tx.vout || []) {
    const addr = vout.scriptPubKey?.addresses?.[0]
    if (addr && watched.has(addr)) addUtxo(addr, tx.txid, vout.n, Math.round(vout.value * 1e8), height)
  }
}

// ── g-268 inclusion/proof rail ──
// We capture a Merkle inclusion proof for a txid only if we have an explicit
// intent for it (POST /proof-intent) OR it pays a watched address (covers our
// own anchors). Proofs are generated from blocks the node hands us — own node
// only, ZERO external indexer in the proof path.
let lastTip = null   // { blockHash, height } — for reorg (prevhash-mismatch) detection

// On-demand abuse guards [pack diff #3]: the unauth /proof on-demand path calls
// getblockhash+getblock, so rate-limit per IP and negative-cache misses. BOTH
// maps are swept of expired entries every 60s so neither grows unbounded.
const onDemandRate = new Map()   // ip   -> { count, resetAt }
const negativeCache = new Map()  // txid -> expiresAt (ms)
const ON_DEMAND_WINDOW_MS = 60_000
const ON_DEMAND_MAX = 10
const NEGATIVE_TTL_MS = 30_000
const _rateSweep = setInterval(() => {
  const now = Date.now()
  for (const [ip, e] of onDemandRate) if (now >= e.resetAt) onDemandRate.delete(ip)
  for (const [t, exp] of negativeCache) if (now >= exp) negativeCache.delete(t)
}, 60_000)
if (_rateSweep.unref) _rateSweep.unref()

function checkOnDemandRate (ip) {
  const now = Date.now()
  let e = onDemandRate.get(ip)
  if (!e || now >= e.resetAt) { e = { count: 0, resetAt: now + ON_DEMAND_WINDOW_MS }; onDemandRate.set(ip, e) }
  e.count++
  return e.count <= ON_DEMAND_MAX
}

function txTouchesWatched (tx) {
  for (const vout of tx.vout || []) {
    const addr = vout.scriptPubKey?.addresses?.[0]
    if (addr && watched.has(addr)) return true
  }
  return false
}

// Build + persist a proof for a txid we believe is confirmed (height from
// seenTxids). Re-fetches the block (verbosity 1 = ordered txids + merkleroot),
// recomputes the root from our branch, and only persists if it matches the
// node's merkleroot. Returns the record, or null (not found / pruned / mismatch).
async function generateProofForConfirmed (txid) {
  const height = seenTxids.get(txid)
  if (height === undefined) return null
  let blockHash, blk
  try { blockHash = await rpc('getblockhash', [height]) } catch { return null }
  try { blk = await rpc('getblock', [blockHash, 1]) } catch { return null }   // tx = [txids], merkleroot
  // TOCTOU guard [pack diff #2]: if a reorg moved this height to a different block
  // between the seenTxids read and now, that block won't contain our txid →
  // indexOf === -1 → null. The caller retries; the new block's ingest has already
  // updated the height (or evicted it on reorg). Self-correcting — so we don't pin
  // blockHash in seenTxids, which g-189's /seen + pruneSeen rely on being txid→height.
  const idx = blk.tx.indexOf(txid)
  if (idx === -1) return null
  const { nodes, index, computedRootInternal } = merkleBranch(blk.tx, idx)
  if (Buffer.from(computedRootInternal).reverse().toString('hex') !== blk.merkleroot) {
    console.error(`[proof] root mismatch for ${txid} @${height} — not persisting`); return null
  }
  const record = { txid, blockHash, height: blk.height, merkleRoot: blk.merkleroot, proof: { nodes, index }, verified: true }
  try { await saveProof(record) } catch (e) { console.error(`[proof] saveProof failed for ${txid}: ${e.message}`); return null }  // [pack C-7]
  return record
}

// ---- ZMQ ingest ----
async function ingest () {
  const sock = new Subscriber()
  sock.connect('tcp://127.0.0.1:28332') // rawtx
  sock.connect('tcp://127.0.0.1:28334') // hashblock
  sock.subscribe('rawtx'); sock.subscribe('hashblock')
  console.log('[indexer] ZMQ ingest live (rawtx + hashblock)')
  for await (const [topicBuf, body] of sock) {
    const topic = topicBuf.toString()
    try {
      if (topic === 'hashblock') {
        const blk = await rpc('getblock', [body.toString('hex'), 2])
        for (const tx of blk.tx) { applyTx(tx, blk.height); if (tx.txid) seenTxids.set(tx.txid, blk.height) }
        pruneSeen(blk.height)
        stats.blocks++; stats.blockTxs += blk.tx.length; stats.lastBlock = blk.height
        console.log(`[indexer] block ${blk.height}: ${blk.tx.length} txs applied (watched=${watched.size})`)

        // g-268 reorg detection: if this block's parent isn't our last tip, the
        // old tip(s) were orphaned. Walk the orphaned chain back to the common
        // ancestor (= the new block's parent), invalidating proofs + evicting
        // txids from seenTxids at each step so a multi-block reorg can't leave a
        // stale proof marked verified or a stale height queryable. [pack diff #1]
        if (lastTip && blk.previousblockhash && blk.previousblockhash !== lastTip.blockHash) {
          let cursor = lastTip.blockHash
          while (cursor && cursor !== blk.previousblockhash) {
            try {
              const orphan = await rpc('getblock', [cursor, 1])
              await markOrphaned(orphan.tx)
              for (const t of orphan.tx) seenTxids.delete(t)
              console.log(`[proof] reorg: orphaned ${orphan.tx.length} txids from height ${orphan.height}`)
              cursor = orphan.previousblockhash
            } catch (e) { console.error(`[proof] reorg walk failed at ${cursor}: ${e.message}`); break }
          }
        }
        lastTip = { blockHash: blk.hash, height: blk.height }

        // g-268 capture-at-ingest: persist proofs for interested txids only.
        const interested = []
        for (let i = 0; i < blk.tx.length; i++) {
          const tx = blk.tx[i]
          if (tx.txid && (isPending(tx.txid) || txTouchesWatched(tx))) interested.push(i)
        }
        if (interested.length > 0) {
          const orderedTxids = blk.tx.map(t => t.txid)
          if (computeMerkleRoot(orderedTxids) !== blk.merkleroot) {
            console.error(`[proof] block ${blk.height} merkleroot mismatch — skipping capture`)
          } else {
            for (const i of interested) {
              const { nodes, index } = merkleBranch(orderedTxids, i)
              // Fire-and-forget [pack diff #4]: disk I/O must not stall ZMQ block ingest.
              saveProof({ txid: orderedTxids[i], blockHash: blk.hash, height: blk.height, merkleRoot: blk.merkleroot, proof: { nodes, index }, verified: true })
                .catch(e => console.error(`[proof] saveProof failed for ${orderedTxids[i]}: ${e.message}`))
            }
            console.log(`[proof] block ${blk.height}: captured ${interested.length} proof(s)`)
          }
        }
      } else if (topic === 'rawtx') {
        applyTx(await rpc('decoderawtransaction', [body.toString('hex')]), -1)
        stats.memTxs++
      }
    } catch (e) { console.log(`[indexer] ingest error (${topic}): ${e.message}`) }
  }
}

// ---- enumeration: local index first, else validated external hint ----
async function unspentFor (addr) {
  watched.add(addr); track(addr)
  const local = utxos.get(addr)
  let candidates = (local && local.size > 0) ? [...local.keys()] : []
  let source = 'index'
  if (candidates.length === 0 && HINT) {
    source = 'hint+gettxout'
    try {
      const r = await fetch(`${HINT}/${addr}/unspent`, { signal: AbortSignal.timeout(10000) })
      if (r.ok) candidates = (await r.json()).map(u => `${u.txid}:${u.vout}`)
    } catch {}
  }
  const out = []
  for (const op of candidates) {
    const [txid, vs] = op.split(':'); const vout = Number(vs)
    const u = await gettxout(txid, vout) // node truth — drops ghosts/spent
    if (u) {
      const sats = Math.round(u.value * 1e8)
      out.push({ tx_hash: txid, tx_pos: vout, value: sats, height: -1 })
      addUtxo(addr, txid, vout, sats, -1)
    } else {
      if (local) local.delete(op) // prune anything the node says is spent
    }
  }
  return { unspent: out, source }
}

// ---- HTTP ----
const ADDR_RE = /^\/unspent\/([13][a-km-zA-HJ-NP-Z1-9]{24,33})$/
const SEEN_RE = /^\/seen\/([0-9a-fA-F]{64})$/
const PROOF_RE = /^\/proof\/([0-9a-fA-F]{64})$/
const server = createServer(async (req, res) => {
  res.setHeader('Content-Type', 'application/json')
  try {
    // GET / (or /dashboard) — zero-dep HTML dashboard (status + in-browser proof verifier + address lookup).
    if (req.method === 'GET' && (req.url === '/' || req.url === '/dashboard') && DASHBOARD) {
      res.setHeader('Content-Type', 'text/html; charset=utf-8')
      res.end(DASHBOARD)
      return
    }
    if (req.method === 'GET' && req.url === '/health') {
      const h = await rpc('getblockchaininfo')
      res.end(JSON.stringify({ ok: true, blocks: h.blocks, pruned: h.pruned, index: { ...stats, watched: watched.size, seen: seenTxids.size } }))
      return
    }
    // g-189 share chain-check: does this txid exist (recently-confirmed OR in mempool)?
    // exists:true is authoritative. exists:false = unknown/out-of-window (caller may fall to WoC).
    const sm = req.url.match(SEEN_RE)
    if (req.method === 'GET' && sm) {
      const txid = sm[1].toLowerCase()
      if (seenTxids.has(txid)) { res.end(JSON.stringify({ txid, exists: true, source: 'block', height: seenTxids.get(txid) })); return }
      try { await rpc('getmempoolentry', [txid]); res.end(JSON.stringify({ txid, exists: true, source: 'mempool', height: -1 })); return } catch {}
      res.end(JSON.stringify({ txid, exists: false, source: 'out-of-window' })); return
    }
    const m = req.url.match(ADDR_RE)
    if (req.method === 'GET' && m) {
      const { unspent, source } = await unspentFor(m[1])
      res.end(JSON.stringify({ address: m[1], source, unspent }))
      return
    }
    // g-268 GET /proof/:txid — real Merkle inclusion proof from our own node.
    // Served from persisted capture, else generated on-demand if the tx is still
    // in the recent (seen) window, else 404. 410 if reorg-orphaned.
    const pm = req.url.match(PROOF_RE)
    if (req.method === 'GET' && pm) {
      const txid = pm[1].toLowerCase()
      // Negative cache [pack diff #3]: a recent miss short-circuits the expensive
      // RPC path so a flood of unknown-txid requests can't hammer the node.
      const negExp = negativeCache.get(txid)
      if (negExp && Date.now() < negExp) {
        res.statusCode = 404
        res.end(JSON.stringify({ txid, error: 'proof not available', reason: isPending(txid) ? 'pending' : 'unknown' }))
        return
      }
      const existing = await loadProof(txid)
      if (existing) {
        if (existing._orphaned) { res.statusCode = 410; res.end(JSON.stringify({ txid, error: 'proof orphaned by reorg' })); return }
        res.end(JSON.stringify(existing)); return
      }
      if (seenTxids.has(txid)) {
        // Per-IP rate limit on the only path that hits node RPC. The indexer is
        // firewalled to the fleet's IPv6 IPs with no proxy, so remoteAddress is
        // the real source. [pack diff #3]
        const ip = req.socket?.remoteAddress || 'unknown'
        if (!checkOnDemandRate(ip)) { res.statusCode = 429; res.end(JSON.stringify({ txid, error: 'rate limit exceeded' })); return }
        const rec = await generateProofForConfirmed(txid)
        if (rec) { res.end(JSON.stringify(rec)); return }
      }
      negativeCache.set(txid, Date.now() + NEGATIVE_TTL_MS)
      res.statusCode = 404
      res.end(JSON.stringify({ txid, error: 'proof not available', reason: isPending(txid) ? 'pending' : 'unknown' }))
      return
    }
    // g-268 POST /proof-intent {txid} — register interest so the proof is captured
    // when the tx confirms. If it's ALREADY confirmed (recent window), compute now
    // (closes the intent-after-ingest race). Fleet-exposure-hardened like /validate.
    if (req.method === 'POST' && req.url === '/proof-intent') {
      const chunks = []; let byteLen = 0
      for await (const c of req) {
        byteLen += c.length
        if (byteLen > (1 << 16)) { res.statusCode = 413; res.end(JSON.stringify({ error: 'body too large' })); req.destroy(); return }
        chunks.push(c)
      }
      let parsed
      try { parsed = JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}') } catch { res.statusCode = 400; res.end(JSON.stringify({ error: 'invalid JSON' })); return }
      const txid = String(parsed.txid || '').toLowerCase()
      if (!/^[0-9a-f]{64}$/.test(txid)) { res.statusCode = 400; res.end(JSON.stringify({ error: 'valid txid required' })); return }
      try { await addPendingIntent(txid) } catch (e) { res.statusCode = 429; res.end(JSON.stringify({ error: e.message })); return }
      if (seenTxids.has(txid)) {
        const rec = await generateProofForConfirmed(txid)
        if (rec) { res.end(JSON.stringify({ queued: false, proof: rec })); return }
      }
      res.statusCode = 202; res.end(JSON.stringify({ queued: true, txid }))
      return
    }
    if (req.method === 'POST' && req.url === '/validate') {
      // g-264 fleet-exposure hardening: this endpoint is reachable by the fleet over
      // IPv6 (unauthenticated, firewalled to fleet IPs), so bound the payload before
      // buffering, cap the batch, and cap RPC pressure. (Content-Type is set globally
      // at the top of this handler.)
      const MAX_BODY = 1 << 20    // 1 MiB, byte-counted
      const MAX_OUTPOINTS = 2000  // generous — real addresses stay well under; >cap 413s
      const RPC_CONCURRENCY = 20  // parallel gettxout slots — caps RPC load on the node
      // Accumulate raw Buffers (avoids Buffer→string coercion + char-vs-byte length bugs).
      const chunks = []
      let byteLen = 0
      for await (const c of req) {
        byteLen += c.length
        if (byteLen > MAX_BODY) { res.statusCode = 413; res.end(JSON.stringify({ error: 'body too large' })); req.destroy(); return }
        chunks.push(c)
      }
      let parsed
      try { parsed = JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}') } catch { res.statusCode = 400; res.end(JSON.stringify({ error: 'invalid JSON' })); return }
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) { res.statusCode = 400; res.end(JSON.stringify({ error: 'object body required' })); return }
      const { outpoints } = parsed
      if (!Array.isArray(outpoints)) { res.statusCode = 400; res.end(JSON.stringify({ error: 'outpoints[] required' })); return }
      if (outpoints.length > MAX_OUTPOINTS) { res.statusCode = 413; res.end(JSON.stringify({ error: `too many outpoints (max ${MAX_OUTPOINTS})` })); return }
      // g-264 B: tip height converts gettxout's `confirmations` into a real block height
      let tip = 0
      try { tip = await rpc('getblockcount') } catch (err) { console.warn('[g264-validate] getblockcount failed — heights will be -1:', err.message) }
      // Parse/sanitize outpoints once.
      const ops = []
      for (const op of outpoints) {
        const [t, v] = String(op).split(':'); const vout = Number(v)
        if (!t || !Number.isInteger(vout) || vout < 0) continue
        ops.push({ t, vout })
      }
      // Bounded parallelism: process in slices of RPC_CONCURRENCY to cap node RPC pressure.
      const unspent = []
      for (let i = 0; i < ops.length; i += RPC_CONCURRENCY) {
        const slice = ops.slice(i, i + RPC_CONCURRENCY)
        const results = await Promise.allSettled(slice.map(async ({ t, vout }) => {
          const u = await gettxout(t, vout)
          if (!u) return null
          const conf = Number(u.confirmations) || 0
          const height = conf > 0 && tip > 0 ? tip - conf + 1 : -1   // >0 confirmed; -1 = mempool OR tip-rpc-down (conservative)
          return { tx_hash: t, tx_pos: vout, value: Math.round(u.value * 1e8), height, address: u.scriptPubKey?.addresses?.[0] || null }
        }))
        for (let j = 0; j < results.length; j++) {
          const r = results[j]
          if (r.status === 'fulfilled' && r.value) unspent.push(r.value)
          else if (r.status === 'rejected') console.error('[g264-validate] outpoint error, skipping:', `${slice[j].t}:${slice[j].vout}`, r.reason?.message)
        }
      }
      res.end(JSON.stringify({ checked: outpoints.length, survived: unspent.length, unspent }))
      return
    }
    res.statusCode = 404; res.end(JSON.stringify({ error: 'not found' }))
  } catch (e) { res.statusCode = 502; res.end(JSON.stringify({ error: e.message })) }
})

await initProofStore()   // g-268: load pending-intent set into memory so isPending() is synchronous
server.listen(PORT, '0.0.0.0', () => console.log(`[indexer] http 0.0.0.0:${PORT} -> node ${RPC_URL} (datadir ${DATADIR})`))
ingest().catch(e => { console.log('[indexer] ingest fatal:', e.message); process.exit(1) })
