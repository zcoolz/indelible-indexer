// chainsync.mjs — pure block-follow decision logic for the indexer (g-286).
// Extracted so the gap-vs-reorg classification is UNIT-TESTABLE without importing
// indexer.mjs (which starts the HTTP server + ZMQ ingest at import). ZERO product
// code; generic chain infrastructure.
//
// The bug this fixes: ZMQ `hashblock` is best-effort and DROPS messages when the
// indexer is starved (e.g. CPU/RAM maxed — Ryan processing spectrograms). On
// resume, the next block's parent won't match our tip — which the old code ALWAYS
// treated as a reorg, walking BACKWARD orphaning every block to the prune floor
// (8,253 canonical blocks wrongly orphaned, ingest wedged). But a parent mismatch
// has TWO causes, told apart by HEIGHT:
//   • blk AHEAD of us (height > lastTip.height + 1)  → we MISSED blocks (gap)
//        → backfill FORWARD from the node's canonical chain. NOT a reorg.
//   • blk AT/BELOW us (height <= lastTip.height + 1) → a fork replaced our tip
//        → bounded reorg walk-back to the common ancestor.

// BSV reorgs are 1–2 blocks in practice; 100 is a paranoid ceiling. Beyond it, a
// "reorg" is really a bug or a mis-detected gap — abort the walk rather than grind
// the whole pruned window to the floor (the original g-268 failure mode).
export const MAX_REORG_DEPTH = 100

// Cap the forward catch-up after falling behind, so a huge gap can't block ZMQ
// ingest for hours. Beyond this we log + jump forward; recent txs still resolve
// on-demand via the seen window.
export const MAX_BACKFILL = 2000

/**
 * Classify a freshly-announced block relative to our current tip. PURE — no I/O.
 * @param {{blockHash:string,height:number}|null} lastTip  our last ingested tip
 * @param {{previousblockhash?:string,height:number}} blk  the new block (getblock)
 * @returns {{action:'cold-start'|'normal'|'gap'|'reorg', from?:number, to?:number}}
 *   gap includes the inclusive backfill range [from,to] of MISSED heights.
 */
export function classifyBlock (lastTip, blk) {
  if (!lastTip) return { action: 'cold-start' }
  // No parent (shouldn't happen post-genesis) or parent links to our tip → contiguous.
  if (!blk.previousblockhash || blk.previousblockhash === lastTip.blockHash) return { action: 'normal' }
  // Parent mismatch: a GAP if the new block is strictly ahead of a normal +1
  // (we missed the blocks in between); otherwise a genuine reorg replaced our tip.
  if (blk.height > lastTip.height + 1) return { action: 'gap', from: lastTip.height + 1, to: blk.height - 1 }
  return { action: 'reorg' }
}

/**
 * Plan which blocks to orphan after a fork, by CANONICAL comparison. Walks back
 * from our tip; a block is orphaned iff its hash !== the node's canonical hash at
 * that height, and the walk STOPS at the first match (the true common ancestor) —
 * so it can never over-orphan canonical history (the original g-268 runaway, where
 * matching blk.previousblockhash could walk past the real ancestor). Bounded by
 * maxDepth. PURE w.r.t. injected lookups → unit-testable without a node. [g-286]
 *
 * @param {string} fromHash    our current tip hash (start of the divergent segment)
 * @param {number} fromHeight  its height
 * @param {(h:number)=>Promise<string|null>} getCanonHash  canonical hash at height h (null if beyond tip)
 * @param {(hash:string)=>Promise<{previousblockhash?:string,tx?:string[]}|null>} getBlock
 * @param {number} [maxDepth=MAX_REORG_DEPTH]
 * @returns {Promise<{toOrphan:Array<{hash:string,height:number,txids:string[]}>, hitCap:boolean}>}
 */
export async function planOrphans (fromHash, fromHeight, getCanonHash, getBlock, maxDepth = MAX_REORG_DEPTH) {
  const toOrphan = []
  let cursor = fromHash
  let h = fromHeight
  let depth = 0
  while (cursor) {
    if (depth >= maxDepth) return { toOrphan, hitCap: true }   // refuse to walk forever — no runaway
    const canon = await getCanonHash(h)
    if (canon && canon === cursor) break                        // reconverged with canonical → common ancestor
    const blk = await getBlock(cursor)
    if (!blk) break                                             // unfetchable (pruned/missing) → stop, bounded
    toOrphan.push({ hash: cursor, height: h, txids: blk.tx || [] })
    cursor = blk.previousblockhash
    h--
    depth++
  }
  return { toOrphan, hitCap: false }
}
