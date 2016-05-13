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
        override fun canMove(shard: ModelShard, destNode: ModelNode, moves: Collection<MoveAction>): Boolean {
            return destNode.shards.none { it.index == shard.index && it.id == shard.id }
        }
    }

    /**
     * Ensure that a shard is in the started state
     */
    val shardAlreadyMovingDecider = object : MockDecider {
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
        override fun canMove(shard: ModelShard, destNode: ModelNode, moves: Collection<MoveAction>): Boolean {
            return moves.none { it.shard.index == shard.index && it.shard.id == shard.id }
        }
    }

    //TODO Add same host decider
}