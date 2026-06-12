# Indelible Indexer — Operator Handbook

A small, generic indexer that runs **next to your own BSV node** and answers the only two questions an app ever asks the chain — without trusting anyone else's index. No app-specific code; stand it up beside your node and read the chain sovereignly.

> Repo: <https://github.com/zcoolz/indelible-indexer> · MIT. The 60-second start is in the [README](./README.md); this is the full operator reference.

**Where it fits.** The indexer is one piece of a small sovereign stack: your full node + this indexer (chain truth) + an [overlay](https://github.com/zcoolz/indelible-overlay) (service discovery), tied together by the **[Relay Federation Bridge](https://github.com/zcoolz/relay-federation)** — an **SPV node** (a bridge *is* an SPV node) that reads chain truth from this indexer *and* advertises its services on the overlay. The bridge is the service layer your apps and agents actually talk to. (Full diagram in the [README](./README.md).)

---

## The two halves

An app asks the chain exactly two things, and they ride different rails:

1. **Balance / UTXO — "is this output unspent?"**
   Answered against your node's own UTXO set (`gettxout`). The node is the truth. An external source may be used only to *enumerate* candidate outpoints for an address with no local history yet — and every candidate is then re-checked through `gettxout` before it's served. The external source is a throwaway hint, never the authority.

2. **Inclusion / proof — "is this transaction really in a block?"**
   Answered with a **Merkle inclusion proof** built from the full block your node hands you at confirmation. The proof recomputes to the block header's merkle root, so anyone can verify it independently against the headers — no need to trust the indexer.

**Invariant:** your own node is the only authority. No external indexer sits in the balance-authority path or the proof path, ever. (Using a propagation service like ARC to *broadcast* is a separate concern.)

---

## Architecture

- **ZMQ ingest.** The node publishes a real-time firehose; the indexer subscribes to `rawtx` (mempool) and `hashblock` (new blocks). On each new block it pulls the full block and updates its state — no polling.
- **Correct chain-follow (gaps vs reorgs).** ZMQ delivery is best-effort, so on a busy or starved box a `hashblock` message can be missed. When a new block's parent isn't the current tip, the indexer tells the two cases apart: a block *ahead* of the tip means missed blocks (a **gap**) → it backfills the missed canonical blocks forward from the node; a block *at or below* the tip means a **reorg** → it orphans only the divergent segment by canonical comparison, walking back exactly to the common ancestor. Both paths are **bounded**, so neither a gap nor a fork can grind the index against canonical history; if a bound is ever hit (it shouldn't be — real BSV reorgs are 1–2 blocks), the indexer sets a **`degraded`** flag visible in `/health` instead of failing silently.
- **Oversized blocks (never wedge).** A mega-block whose verbosity-2 JSON exceeds Node's ~512 MB string cap would otherwise throw on fetch and wedge the backfill loop — re-fetching the huge block every tick (which can even OOM the node serving it). Instead the indexer falls back to a **verbosity-1 "light" ingest**: it records the block's txids + captures any proof-*intents* you registered, but skips the full UTXO-apply for that one block (`gettxout` stays the UTXO authority, so the next watched-address query self-corrects). The tip advances past any block; the light ingest shows as `degraded` in `/health`.
- **Forward UTXO index (in-memory).** Per-address UTXO tracking for addresses you've queried, spend-tracked (an input that spends a tracked output removes it).
- **Recently-seen window (in-memory).** `txid → height` for the last ~144 blocks (~24h), so you can answer "is this confirmed (or in mempool) right now?" quickly. Rebuilds from the firehose after a restart.
- **Proof store (on disk, persistent).** One small file per captured proof, written atomically. This must persist: a pruned node won't have the block later, so the proof is captured the moment the block arrives and kept.
- **Capture is selective.** A proof is built only for a txid you've registered interest in (`/proof-intent`) or one that pays an address you watch — so a pruned node serving a focused set of anchors stays light. The block's merkle tree is built once per block, only when there's something to capture.

---

## Endpoint API

| Method / path | Purpose | Success response |
|---|---|---|
| `GET /` | zero-dep web dashboard (status + in-browser proof verifier + address lookup) | `text/html` |
| `GET /health` | node + index stats | `{ ok, blocks, pruned, index:{ blocks, blockTxs, memTxs, lastBlock, degraded, watched, seen } }` |
| `GET /unspent/:address` | address UTXOs (node-validated) | `{ address, source, unspent:[{ tx_hash, tx_pos, value, height }] }` |
| `POST /validate` `{ outpoints:["txid:vout", …] }` | filter outpoints to those still unspent (per `gettxout`) | `{ checked, survived, unspent:[{ tx_hash, tx_pos, value, height, address }] }` |
| `GET /seen/:txid` | recently-confirmed or in mempool? | `{ txid, exists, source:"block"\|"mempool"\|"out-of-window", height }` |
| `GET /proof/:txid` | **Merkle inclusion proof** | `{ txid, blockHash, height, merkleRoot, proof:{ nodes:[…], index }, verified:true }` · `404 {reason:"pending"\|"unknown"}` · `410` if the block was reorged out |
| `POST /proof-intent` `{ txid }` | register interest so the proof is captured when the tx confirms | `202 {queued:true}` · or `{queued:false, proof}` if already confirmed |

Notes:
- `POST /validate` is bounded (outpoint count + body size capped). `GET /proof` on-demand generation is per-IP rate-limited and negative-cached, since it touches node RPC.
- Call `POST /proof-intent` right after you broadcast an anchor; the proof is then captured automatically at confirmation. `GET /proof` also generates on demand if the tx is still in the recent window.

### The dashboard (`GET /`)
Open `http://<indexer-host>:9201/` for a zero-dependency web dashboard: live node + index **status**, a **proof explorer** (paste a txid → it fetches `/proof` and **recomputes the Merkle root in your browser**, checking it against the block header — *don't trust, verify*), and a node-validated **address lookup**. The in-browser verify needs a secure context, so use `http://localhost` / `127.0.0.1` or HTTPS; on a plain-HTTP LAN IP it shows a note to open via localhost/HTTPS instead.

### Verifying a proof yourself
A returned proof is independently checkable — don't trust the indexer, verify it:
1. Start from the txid (internal/little-endian byte order).
2. For each sibling in `proof.nodes` (also reversed to internal order), fold: if the current index is even, `dsha256(self ‖ sibling)`, else `dsha256(sibling ‖ self)`; then `index = floor(index / 2)`.
3. Reverse the result to display order and check it equals `merkleRoot` — and that `merkleRoot` is the header of the block at `height`.

(`dsha256` = SHA-256 applied twice.)

---

## Run your own

Clone the repo — files: `indexer.mjs`, `merkle.mjs`, `proofStore.mjs`, `dashboard.html`, and `package.json` (one dependency: `zeromq`):
```
git clone https://github.com/zcoolz/indelible-indexer.git
cd indelible-indexer
npm install
```
> `zeromq` is a native module — on Linux install `build-essential` + `python3` first so it can build.

**Node requirements**
- A **full** `bitcoind` node (pruned is fine) — **not** an SPV / headers-only client. The indexer reads the UTXO set (`gettxout`), the mempool/raw-tx + block ZMQ firehose, and full blocks (`getblock`); a headers-only node has none of those.
- ZMQ enabled — the indexer subscribes to `rawtx` and `hashblock`:
  ```
  zmqpubrawtx=tcp://127.0.0.1:28332
  zmqpubhashblock=tcp://127.0.0.1:28334
  ```
- RPC reachable on loopback — the indexer reads credentials from your node's `bitcoin.conf` (`server=1`, `rpcuser`, `rpcpassword`).
- `-txindex` is **not** required — proofs are built from full blocks at ingest. A **pruned** node is fine for forward operation; proofs for anchors registered from then on are captured and kept. (Anchors older than your prune window that were never registered would need a one-time fetch from an archival peer — out of scope here.)

**Config (env)**
- `BSV_DATADIR` — your node's data directory (default `/data/bitcoin`).
- `PORT` — HTTP port (default `9201`).
- `PROOFS_DIR` — where proofs persist (default `./proofs`).
- `GP_HINT` — optional cold-address enumeration source, **off by default** (out of the box the indexer calls no third party; `/unspent` serves only what its forward index has already seen). Set it to an **`https://`** source you trust to enumerate UTXOs for addresses with no local history yet; an invalid or non-https value is ignored. Every candidate it returns is still re-validated through your own node, so it is never an authority.

**Run**
```
BSV_DATADIR=/path/to/node PORT=9201 node indexer.mjs
# or as a service (systemd): ExecStart=node /opt/indelible-indexer/indexer.mjs, Restart=on-failure
```

**Expose it carefully.** `/validate` and on-demand `/proof` are unauthenticated (bounded + rate-limited). Firewall `:9201` to the consumers you trust — typically your own bridge/app on the same host or network. Co-locate the indexer with the node over loopback so there's zero distance between the index and the truth.

---

## Verify it's working
```
curl -s http://127.0.0.1:9201/health
curl -s http://127.0.0.1:9201/seen/<txid>
curl -s http://127.0.0.1:9201/proof/<txid>
```
A `/proof` that returns `verified:true` and recomputes (per "Verifying a proof yourself") to the block's merkle root means the rail is sound end to end.

---

## Recovery — common cases
| Symptom | Likely cause | Action |
|---|---|---|
| `/health` unreachable | indexer process not running | restart it; check its log |
| `/health` ok but `blocks`/`lastBlock` stale | node stopped, or ZMQ not flowing | confirm the node is up and ZMQ ports are listening; restart the node cleanly if needed |
| `/validate` returns unvalidated candidates | node RPC unavailable | transient — restore the node; reads degrade gracefully meanwhile |
| `/proof/:txid` stuck on `pending` | the tx hasn't been mined yet | confirm it's in the mempool; only re-broadcast if it's genuinely absent (never re-broadcast a tx that already relayed) |
| `/proof/:txid` returns `410` | the block was reorged; the proof was invalidated | wait for the tx to re-confirm in the new chain; a fresh intent re-captures it |
| `/health` shows `index.degraded` set | the indexer fell behind beyond its backfill bound, a reorg exceeded the walk depth, **or an oversized block was light-ingested** (all rare) | restart to resync forward from the node; a deep reorg that left proofs partial → clear the proof store for the affected range + re-register intents. A light-ingested block still serves proofs-by-intent + `gettxout`-validated UTXO reads. |

---

*After a restart, the in-memory pieces (recently-seen window, forward index) rebuild from the firehose over time; the on-disk proofs persist immediately. While the window refills, "is this seen?" answers conservatively (treat an out-of-window miss as "unknown," not "absent").*

Own the node. Own the index. Own the proofs.
