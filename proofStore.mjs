// proofStore.mjs — atomic flat-file persistence for inclusion proofs + pending
// intents (g-268). Generic chain infra (ZERO Indelible product code).
//
// Durability contract [pack #3]:
//   - atomicWrite: tmp -> fsync(fd) -> close -> rename -> fsync(dir_fd).
//     The directory fsync closes the rename-durability gap on ext4/xfs.
//     tmp is cleaned up on ANY failure, including a rename failure [pack C-7].
//   - All _pending.json mutations serialize through one async queue so the
//     ingest path and a /proof-intent request can't interleave and corrupt it.
//   - _pendingSet is held in memory; isPending() is a SYNCHRONOUS check [pack C-4]
//     (the async version inside a .filter() callback was the V2 bug).
//
// Reorg [pack #5]: indexer.mjs detects the fork (prevhash mismatch) and calls
// markOrphaned(txids). We rename <txid>.json -> <txid>.orphaned (audit trail,
// not deletion); loadProof flags it; GET /proof serves 410 for orphaned entries.

import fs from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const PROOFS_DIR = process.env.PROOFS_DIR || path.join(__dirname, 'proofs')
const PENDING_FILE = path.join(PROOFS_DIR, '_pending.json')
const MAX_PENDING = 10_000
const TXID_RE = /^[0-9a-fA-F]{64}$/

async function ensureDir () { await fs.mkdir(PROOFS_DIR, { recursive: true }) }

// fsync the directory entry to make a rename durable. This is the real
// durability close on Linux ext4/xfs (the deploy target). Some platforms/
// filesystems refuse fsync on a directory handle (Windows EPERM, others
// EINVAL/EISDIR/ENOSYS) — there it's a no-op we degrade to gracefully; the
// rename still provides atomicity. Real errors still propagate.
async function fsyncDir (dir) {
  let dirFd
  try {
    dirFd = await fs.open(dir, 'r')
    await dirFd.sync()
  } catch (e) {
    if (!['EPERM', 'EINVAL', 'EISDIR', 'ENOSYS', 'ENOTSUP'].includes(e.code)) throw e
  } finally {
    if (dirFd) await dirFd.close()
  }
}

// Write content atomically: tmp -> fsync -> rename -> fsync(dir). Cleans up tmp
// on any failure (including rename), so a half-written tmp never lingers.
async function atomicWrite (filePath, content) {
  const dir = path.dirname(filePath)
  const tmp = `${filePath}.tmp.${process.pid}.${Date.now()}`
  let fd
  try {
    fd = await fs.open(tmp, 'w')
    await fd.write(content)
    await fd.sync()
    await fd.close(); fd = null
    await fs.rename(tmp, filePath)
    await fsyncDir(dir)
  } catch (err) {
    if (fd) { try { await fd.close() } catch {} }
    try { await fs.unlink(tmp) } catch {}
    throw err
  }
}

// ── pending intents (in-memory set + serialized persistence) ──
let _pendingSet = null
let _pendingWriteQueue = Promise.resolve()

// Load _pending.json into memory ONCE at boot, so isPending() can be synchronous.
export async function initProofStore () {
  await ensureDir()
  if (_pendingSet !== null) return
  try {
    _pendingSet = new Set(JSON.parse(await fs.readFile(PENDING_FILE, 'utf8')))
  } catch (e) {
    if (e.code !== 'ENOENT') throw e
    _pendingSet = new Set()
  }
}

function enqueuePendingWrite (fn) {
  _pendingWriteQueue = _pendingWriteQueue.then(fn).catch(err => {
    console.error('[proofStore] pending write error:', err.message)
  })
  return _pendingWriteQueue
}

/** SYNCHRONOUS — safe inside .filter()/loops. Returns false if not yet init'd. */
export function isPending (txid) {
  return _pendingSet !== null && _pendingSet.has(txid.toLowerCase())
}

export async function addPendingIntent (txid) {
  if (!TXID_RE.test(txid)) throw new Error('invalid txid')
  if (_pendingSet === null) await initProofStore()
  const t = txid.toLowerCase()
  if (_pendingSet.has(t)) return            // idempotent
  if (_pendingSet.size >= MAX_PENDING) throw new Error('pendingProofs cap reached')
  _pendingSet.add(t)
  return enqueuePendingWrite(() => atomicWrite(PENDING_FILE, JSON.stringify([..._pendingSet])))
}

export async function removePendingIntent (txid) {
  if (_pendingSet === null) await initProofStore()
  const t = txid.toLowerCase()
  if (!_pendingSet.has(t)) return
  _pendingSet.delete(t)
  return enqueuePendingWrite(() => atomicWrite(PENDING_FILE, JSON.stringify([..._pendingSet])))
}

// ── proof persistence ──
const proofPath = (txid) => path.join(PROOFS_DIR, `${txid.toLowerCase()}.json`)
const orphanedPath = (txid) => path.join(PROOFS_DIR, `${txid.toLowerCase()}.orphaned`)

/** proofRecord: { txid, blockHash, height, merkleRoot, proof:{nodes,index}, verified } */
export async function saveProof (proofRecord) {
  await ensureDir()
  await atomicWrite(proofPath(proofRecord.txid), JSON.stringify(proofRecord))
  await removePendingIntent(proofRecord.txid)
}

/** Returns the record, { ...rec, _orphaned:true } if reorg-invalidated, or null. */
export async function loadProof (txid) {
  try {
    return { ...JSON.parse(await fs.readFile(orphanedPath(txid), 'utf8')), _orphaned: true }
  } catch (e) { if (e.code !== 'ENOENT') throw e }
  try {
    return JSON.parse(await fs.readFile(proofPath(txid), 'utf8'))
  } catch (e) { if (e.code !== 'ENOENT') throw e; return null }
}

/** Rename each <txid>.json -> <txid>.orphaned. Each rename is individually
 *  atomic; partial completion on crash is safe (loadProof checks .orphaned first). */
export async function markOrphaned (txids) {
  for (const txid of txids) {
    try {
      await fs.rename(proofPath(txid), orphanedPath(txid))
      await fsyncDir(PROOFS_DIR)
    } catch (e) {
      if (e.code !== 'ENOENT') throw e   // not-present = nothing we proved; skip
    }
  }
}
