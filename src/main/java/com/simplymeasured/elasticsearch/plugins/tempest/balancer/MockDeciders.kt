package com.simplymeasured.elasticsearch.plugins.tempest.balancer

import org.elasticsearch.cluster.routing.ShardRoutingState
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider

/**
 * Created by awhite on 4/17/16.
 */

object MockDeciders {
    val sameNodeDecider = object : MockDecider {
        override fun canMove(shard: ModelShard, destNode: ModelNode, moves: Collection<MoveAction>): Boolean {
            return destNode.shards.none { it.index == shard.index && it.id == shard.id }
        }
    }

    val shardAlreadyMovingDecider = object : MockDecider {
        override fun canMove(shard: ModelShard, destNode: ModelNode, moves: Collection<MoveAction>): Boolean {
            return shard.state == ShardRoutingState.STARTED
        }
    }

    val shardIdAlreadyMoving = object : MockDecider {
        override fun canMove(shard: ModelShard, destNode: ModelNode, moves: Collection<MoveAction>): Boolean {
            return moves.none { it.shard.index == shard.index && it.shard.id == shard.id }
        }
    }

    //TODO Add same host decider
}