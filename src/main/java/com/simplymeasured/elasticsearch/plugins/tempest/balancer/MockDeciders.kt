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

import org.elasticsearch.cluster.routing.ShardRoutingState
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider

/**
 * A set of basic mock deciders
 */
object MockDeciders {

    /**
     * Ensure a shard does not end up on the same node as one of it's replicas
     */
    val sameNodeDecider = object : MockDecider {
        override fun canAllocate(shard: ModelShard, destNode: ModelNode): Boolean {
            return destNode.shards.none { it.index == shard.index && it.id == shard.id }
        }

        override fun canMove(shard: ModelShard, destNode: ModelNode, moves: Collection<MoveAction>): Boolean {
            return canAllocate(shard, destNode)
        }
    }

    /**
     * Ensure that a shard is in the correct state
     */
    val shardStateDecider = object : MockDecider {
        override fun canAllocate(shard: ModelShard, destNode: ModelNode): Boolean {
            return shard.state == ShardRoutingState.UNASSIGNED
        }

        override fun canMove(shard: ModelShard, destNode: ModelNode, moves: Collection<MoveAction>): Boolean {
            return shard.state == ShardRoutingState.STARTED
        }
    }

    /**
     * Ensure that a only one shard of a given id is moving
     *
     * This decider is not part of ES's core deciders but is included based on experiences where moving multiple
     * shards with the same id caused significant performance issues
     */
    val shardIdAlreadyMoving = object : MockDecider {
        override fun canAllocate(shard: ModelShard, destNode: ModelNode): Boolean = true

        override fun canMove(shard: ModelShard, destNode: ModelNode, moves: Collection<MoveAction>): Boolean {
            return moves.none { it.shard.index == shard.index && it.shard.id == shard.id }
        }
    }

    //TODO Add same host decider
}