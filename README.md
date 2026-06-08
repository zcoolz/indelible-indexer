# 🛰️ Indelible Indexer

**Sovereign chain truth for BSV — read the blockchain from _your own node_, not someone else's index.**

![license](https://img.shields.io/badge/license-MIT-black) ![node](https://img.shields.io/badge/node-%E2%89%A518-orange) ![chain](https://img.shields.io/badge/chain-BSV-amber)

> *Own the node · own the index · own the proofs. Don't trust — verify.*

---

Most apps read the blockchain through a third-party indexer — and inherit its mistakes: stale spends, ghost UTXOs, an outage you can't fix, a version of "truth" you don't control. **Indelible Indexer removes the middleman.** It runs right next to your own BSV node and serves chain truth straight from it.

It's small, dependency-light, and **carries zero application code** — generic infrastructure. If you run a node, you can run this beside it and answer the only two questions any app ever asks the chain:

### The two questions
- 💰 **"Is this output unspent?"** — validated against your node's own UTXO set (`gettxout`). No ghosts, no guesses.
- 🔗 **"Is this transaction really in a block?"** — a **Merkle inclusion proof**, built from the full block your node hands over at confirmation, and **independently verifiable** — even in a browser.

Balance truth and inclusion truth, both sovereign. That's the whole product.

## ⚡ Quick start
```bash
# 1) Enable RPC + ZMQ on your BSV node (bitcoin.conf):
#      server=1
#      rpcuser=…  /  rpcpassword=…
#      zmqpubrawtx=tcp://127.0.0.1:28332
#      zmqpubhashblock=tcp://127.0.0.1:28334

# 2) Run the indexer beside it:
git clone https://github.com/zcoolz/indelible-indexer.git
cd indelible-indexer && npm install
BSV_DATADIR=/data/bitcoin npm start

# 3) Verify:
curl http://127.0.0.1:9201/health
```
> ⚠️ **Needs a *full* bitcoind node** (pruned is fine, ~50 GB) — an **SPV / headers-only** client can't feed the indexer: it reads the UTXO set (`gettxout`), the mempool/raw-tx + block ZMQ firehose, and full blocks (`getblock`), none of which a headers-only node has.

Full setup, environment, systemd, and recovery → **[Operator Handbook](./HANDBOOK.md)**.

## 🔎 Don't trust, verify — in your browser
Open `http://127.0.0.1:9201/` for the built-in **Proof Explorer**: paste a txid and it fetches the inclusion proof, then **recomputes the Merkle root locally** (`crypto.subtle`) and checks it against the block header — right there in your browser. Plus live node status and node-validated address lookups. Zero dependencies, served by the indexer itself.

## 🧩 API at a glance
| Method · path | Answers |
|---|---|
| `GET /health` | node + index status |
| `GET /unspent/:addr` | node-validated UTXOs for an address |
| `POST /validate` | filter outpoints down to the truly-unspent set |
| `GET /seen/:txid` | is this tx in the mempool or a recent block? |
| `GET /proof/:txid` | Merkle inclusion proof `{blockHash, height, merkleRoot, proof}` |
| `POST /proof-intent` | capture a proof the moment a tx confirms |

## 🏛️ How it fits
```
        your BSV node            (the source of truth)
              │  loopback RPC + ZMQ
        ┌─────▼─────────┐
        │   Indelible   │        ← this repo  (HTTP :9201)
        │    Indexer    │
        └─────┬─────────┘
              │  HTTP (firewalled to you)
     your bridge · app · wallet  (the consumer)
```
The indexer is one of three small, independent pieces you can run sovereignly:
- **indelible-indexer** — this repo (chain truth + inclusion proofs).
- **bsv-node-dashboard** — a zero-dependency monitor for the node itself *(separate repo)*.
- a federation **bridge** — the service layer your apps talk to.

Run only what you need; each is independent and speaks plain HTTP.

## 🔒 Sovereignty notes
Your node is the only authority — out of the box the indexer calls **no third party at all**. Cold-address enumeration is opt-in (`GP_HINT`, https-only, off by default); even when you enable it, every candidate it returns is re-validated through your node, and it never touches the proof path. Firewall `:9201` to your own consumers; it's built to sit behind a trust boundary.

## 📄 License
[MIT](./LICENSE) — run it, fork it, ship it. Built by the **Indelible Federation**.
