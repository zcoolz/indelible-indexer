// merkle.mjs — Merkle inclusion-proof generator for BSV (g-268).
// Generic chain infra (destined for relay-federation; ZERO Indelible product code).
//
// Byte-order contract (CRITICAL — must round-trip with the bridge's
// verifyMerkleProof in bridge/lib/persistent-store.js):
//   INPUT  txidsDisplayHex[] — txids as displayed (big-endian hex, 64 chars)
//   INTERNAL                 — reversed to little-endian Buffers for tree ops
//   OUTPUT nodes[]           — siblings as DISPLAY hex (reversed back)
//
// The bridge's verifyMerkleProof does, per sibling:
//   hash    = reverse(txHash)          // display hex -> LE
//   sibling = reverse(proofHash)       // display hex -> LE
//   combined = index%2==0 ? hash||sibling : sibling||hash
//   hash    = dsha256(combined); index = floor(index/2)
//   assert reverse(hash).hex == header.merkleRoot
// So we compute in LE and emit siblings as display hex; verify reverses them
// back. Correct IFF we never double-reverse. (merkle.test.mjs proves this
// against a simulateVerify() that mirrors the bridge byte-for-byte.)
//
// Duplicate-last-node rule (CVE-2012-2459 / Bitcoin Core ComputeMerkleRoot):
// at EVERY level with an odd node count, duplicate the last node before pairing.

import { createHash } from 'node:crypto'

function dsha256 (buf) {
  return createHash('sha256').update(createHash('sha256').update(buf).digest()).digest()
}

/** display hex string -> 32-byte LE Buffer */
function displayToInternal (hexStr) {
  return Buffer.from(hexStr, 'hex').reverse()
}

/** 32-byte LE Buffer -> display hex string */
function internalToDisplay (buf) {
  return Buffer.from(buf).reverse().toString('hex')
}

/**
 * computeMerkleRoot(txidsDisplayHex) -> display-hex root.
 * Single tx: root == txid (no hashing). O(n) hashing otherwise.
 */
export function computeMerkleRoot (txidsDisplayHex) {
  if (!txidsDisplayHex || txidsDisplayHex.length === 0) throw new Error('computeMerkleRoot: empty txids')
  if (txidsDisplayHex.length === 1) return txidsDisplayHex[0].toLowerCase()
  let level = txidsDisplayHex.map(displayToInternal)
  while (level.length > 1) {
    if (level.length % 2 === 1) level.push(Buffer.from(level[level.length - 1]))
    const next = []
    for (let i = 0; i < level.length; i += 2) next.push(dsha256(Buffer.concat([level[i], level[i + 1]])))
    level = next
  }
  return internalToDisplay(level[0])
}

/**
 * merkleBranch(txidsDisplayHex, index) -> { nodes, index, computedRootInternal }
 *   nodes : sibling hashes in DISPLAY hex, leaf-to-root order
 *   index : the original tx index (verifyMerkleProof needs it)
 *   computedRootInternal : LE Buffer of the recomputed root (assert before persisting)
 * Throws on empty txids or out-of-range index.
 */
export function merkleBranch (txidsDisplayHex, index) {
  if (!txidsDisplayHex || txidsDisplayHex.length === 0) throw new Error('merkleBranch: empty txids')
  if (!Number.isInteger(index) || index < 0 || index >= txidsDisplayHex.length) {
    throw new Error(`merkleBranch: index ${index} out of range ${txidsDisplayHex.length}`)
  }

  let level = txidsDisplayHex.map(displayToInternal)
  let idx = index
  const nodes = []

  while (level.length > 1) {
    if (level.length % 2 === 1) level.push(Buffer.from(level[level.length - 1])) // duplicate-last at EVERY odd level
    const siblingIdx = idx % 2 === 0 ? idx + 1 : idx - 1
    nodes.push(internalToDisplay(level[siblingIdx]))
    const next = []
    for (let i = 0; i < level.length; i += 2) next.push(dsha256(Buffer.concat([level[i], level[i + 1]])))
    idx = Math.floor(idx / 2)
    level = next
  }

  // Single-tx block: loop never runs, nodes=[], level[0] is the lone txid (LE).
  return { nodes, index, computedRootInternal: level[0] }
}

/**
 * selfTestBlock(txidsDisplayHex, merkleRootDisplay) — used by the test suite.
 * Asserts computeMerkleRoot matches AND every branch round-trips to the root.
 * (Production code in indexer.mjs uses the lighter computeMerkleRoot-once +
 * per-persisted-branch assert to avoid O(n^2) on large blocks.)
 */
export function selfTestBlock (txidsDisplayHex, merkleRootDisplay) {
  const want = merkleRootDisplay.toLowerCase()
  const computed = computeMerkleRoot(txidsDisplayHex)
  if (computed !== want) throw new Error(`selfTestBlock: computeMerkleRoot mismatch: got ${computed} want ${want}`)
  const rootInternal = displayToInternal(want)
  for (let i = 0; i < txidsDisplayHex.length; i++) {
    const { computedRootInternal } = merkleBranch(txidsDisplayHex, i)
    if (!computedRootInternal.equals(rootInternal)) throw new Error(`selfTestBlock: branch mismatch at index ${i}`)
  }
}

export { dsha256 as _dsha256 }
