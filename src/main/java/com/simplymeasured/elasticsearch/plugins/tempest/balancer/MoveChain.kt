/*
 * The MIT License (MIT)
 * Copyright (c) 2016 DataRank, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 *
 */

package com.simplymeasured.elasticsearch.plugins.tempest.balancer

import org.eclipse.collections.api.list.ListIterable
import org.eclipse.collections.api.list.MutableList
import org.eclipse.collections.impl.factory.Lists

/**
 * MoveChains represent a series of move batches and also capture some end-state values for the cluster if the chain
 * is applied
 */
class MoveChain private constructor(val moveBatches: ListIterable<MoveActionBatch>, val overhead: Long, val risk: Double, val score: Double) {
    companion object {
        private val INVALID_MOVE_CHAIN = MoveChain(Lists.mutable.empty<MoveActionBatch>(), Long.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE)

        /**
         * Attempt to build the best sub-chain for a set of move batches
         *
         * Often a long chain of move batches will have an optimal "head" and sub-optimal tail. This method finds the
         * the score in the list and culls all moves after that point.
         *
         * Likewise the risk is set to the maximum encountered risk in the optimal sublist
         */
        fun fromMoveBatches(moveBatches: ListIterable<MoveActionBatch>) : MoveChain {
            return moveBatches
                    .reject { it.moves.isEmpty }
                    .let { findOptimalLeadingSubChain(it)}
        }

        private fun findOptimalLeadingSubChain(moveBatches: ListIterable<MoveActionBatch>): MoveChain {
            when (moveBatches.size()) {
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

        fun noopMoveChain(score: Double): MoveChain {
            return MoveChain(
                    moveBatches = Lists.immutable.empty(),
                    overhead = 0L,
                    risk = 0.0,
                    score = score)
        }
    }
}