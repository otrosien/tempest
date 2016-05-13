package com.simplymeasured.elasticsearch.plugins.tempest.balancer

import org.eclipse.collections.api.list.ListIterable
import org.eclipse.collections.impl.factory.Lists

/**
 * MoveChains represent a series of move batches and also capture some end-state values for the cluster if the chain
 * is applied
 */
class MoveChain private constructor(val moveBatches: List<MoveActionBatch>, val overhead: Long, val risk: Double, val score: Double) {
    companion object {
        val INVALID_MOVE_CHAIN = MoveChain(Lists.mutable.empty<MoveActionBatch>(), 0, 0.0, Double.MAX_VALUE)

        /**
         * Attempt to build the best sub-chain for a set of move batches
         *
         * Often a long chain of move batches will have an optimal "head" and sub-optimal tail. This method finds the
         * the score in the list and culls all moves after that point.
         *
         * Likewise the risk is set to the maximum encountered risk in the optimal sublist
         */
        fun buildOptimalMoveChain(moveBatches: MutableList<MoveActionBatch>) : MoveChain {
            when (moveBatches.size) {
                0 -> return INVALID_MOVE_CHAIN
                1 -> return moveBatches.first().let { MoveChain(moveBatches, it.overhead, it.risk, it.score) }
            }

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