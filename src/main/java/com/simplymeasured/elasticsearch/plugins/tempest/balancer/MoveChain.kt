package com.simplymeasured.elasticsearch.plugins.tempest.balancer

class MoveChain private constructor(val moveBatches: List<MoveActionBatch>, val overhead: Long, val risk: Double, val score: Double) {
    companion object {
        fun buildOptimalMoveChain(moveBatches: MutableList<MoveActionBatch>) : MoveChain {
            val bestIndex = moveBatches.mapIndexed { index, moveActionBatch -> Pair(index, moveActionBatch.score) }
                                       .minBy { it.second }
                                       ?.first ?: 0

            val optimalSublist = moveBatches.subList(0, bestIndex + 1)
            val score = moveBatches.get(bestIndex).score
            val overhead = optimalSublist.map { it.overhead }.sum()
            val risk = optimalSublist.map { it.risk }.max() ?: 0.0

            return MoveChain(optimalSublist, overhead, risk, score)
        }
    }
}