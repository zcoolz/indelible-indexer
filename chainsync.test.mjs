// chainsync.test.mjs — g-286. Verifies the gap-vs-reorg classification that the
// runaway-orphaning bug got wrong. Pure logic, no node/ZMQ needed.
import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { classifyBlock, planOrphans, MAX_REORG_DEPTH, MAX_BACKFILL } from './chainsync.mjs'

describe('classifyBlock (g-286 gap-vs-reorg)', () => {
  it('cold-start when there is no tip yet', () => {
    assert.equal(classifyBlock(null, { height: 100, previousblockhash: 'X' }).action, 'cold-start')
  })

  it('normal advance: parent === tip', () => {
    assert.equal(classifyBlock({ blockHash: 'A', height: 100 }, { height: 101, previousblockhash: 'A' }).action, 'normal')
  })

  it('missing previousblockhash → treated as normal (cannot infer a fork)', () => {
    assert.equal(classifyBlock({ blockHash: 'A', height: 100 }, { height: 101 }).action, 'normal')
  })

  it('GAP: block is ahead (missed blocks) → backfill window [from,to]', () => {
    const c = classifyBlock({ blockHash: 'A', height: 100 }, { height: 120, previousblockhash: 'Z' })
    assert.equal(c.action, 'gap'); assert.equal(c.from, 101); assert.equal(c.to, 119)
  })

  it('GAP of exactly one missed block', () => {
    const c = classifyBlock({ blockHash: 'A', height: 100 }, { height: 102, previousblockhash: 'Z' })
    assert.equal(c.action, 'gap'); assert.equal(c.from, 101); assert.equal(c.to, 101)
  })

  it('REORG: 1-block tip replacement (height+1, different parent)', () => {
    assert.equal(classifyBlock({ blockHash: 'A', height: 100 }, { height: 101, previousblockhash: 'B' }).action, 'reorg')
  })

  it('REORG: same-height sibling', () => {
    assert.equal(classifyBlock({ blockHash: 'A', height: 100 }, { height: 100, previousblockhash: 'B' }).action, 'reorg')
  })

  it('REORG: deeper (new block below our tip)', () => {
    assert.equal(classifyBlock({ blockHash: 'A', height: 100 }, { height: 98, previousblockhash: 'B' }).action, 'reorg')
  })

  it('THE BUG: a 20-block gap (952832 → 952852) classifies as GAP, not reorg', () => {
    // The exact live failure: tip wedged at 952832, node at 952852.
    const c = classifyBlock({ blockHash: 'TIP832', height: 952832 }, { height: 952852, previousblockhash: 'PREV851' })
    assert.equal(c.action, 'gap'); assert.equal(c.from, 952833); assert.equal(c.to, 952851)
  })

  it('bound constants are sane', () => {
    assert.ok(MAX_REORG_DEPTH >= 10 && MAX_REORG_DEPTH <= 1000, 'reorg cap in a sane range')
    assert.ok(MAX_BACKFILL >= 100, 'backfill cap allows real catch-up')
  })
})

describe('planOrphans (g-286 canonical-comparison fork walk)', () => {
  it('1-block reorg: orphans only our stale tip, stops at the common ancestor', async () => {
    // our tip A@100 (parent X@99); canonical is now B@100 (parent X@99).
    const canon = { 100: 'B', 99: 'X' }
    const blocks = { A: { previousblockhash: 'X', tx: ['a1', 'a2'] } }
    const { toOrphan, hitCap } = await planOrphans('A', 100, async h => canon[h] ?? null, async hash => blocks[hash] ?? null)
    assert.equal(hitCap, false)
    assert.deepEqual(toOrphan.map(b => b.hash), ['A'])        // A only — NEVER the canonical ancestor X
    assert.deepEqual(toOrphan[0].txids, ['a1', 'a2'])
  })

  it('deeper reorg: orphans the whole divergent segment down to the ancestor', async () => {
    // our chain X@98 <- C@99 <- A@100 ; canonical X@98 <- D@99 <- B@100
    const canon = { 100: 'B', 99: 'D', 98: 'X' }
    const blocks = { A: { previousblockhash: 'C', tx: ['a'] }, C: { previousblockhash: 'X', tx: ['c'] } }
    const { toOrphan } = await planOrphans('A', 100, async h => canon[h] ?? null, async hash => blocks[hash] ?? null)
    assert.deepEqual(toOrphan.map(b => b.hash), ['A', 'C'])   // stops at X (canonical match)
  })

  it('shorter canonical chain (our block beyond tip) → orphan it', async () => {
    const canon = { 100: null, 99: 'X' }                      // canonical tip is 99
    const blocks = { A: { previousblockhash: 'X', tx: ['a'] } }
    const { toOrphan } = await planOrphans('A', 100, async h => canon[h] ?? null, async hash => blocks[hash] ?? null)
    assert.deepEqual(toOrphan.map(b => b.hash), ['A'])
  })

  it('no fork: tip already canonical → orphans nothing', async () => {
    const { toOrphan } = await planOrphans('A', 100, async () => 'A', async () => ({ previousblockhash: 'X', tx: [] }))
    assert.equal(toOrphan.length, 0)
  })

  it('hitCap: refuses to walk past maxDepth — NO runaway (the original bug)', async () => {
    // every height non-canonical, chain links forever → must stop at maxDepth, not the prune floor
    const { toOrphan, hitCap } = await planOrphans(
      'h0', 1000,
      async () => 'CANON-NEVER-MATCHES',
      async hash => ({ previousblockhash: 'p' + hash, tx: [] }),
      5
    )
    assert.equal(hitCap, true)
    assert.equal(toOrphan.length, 5)
  })
})
