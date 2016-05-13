package com.simplymeasured.elasticsearch.plugins.tempest.balancer

/**
 * Interface that defines a very simple decider to be during move simulations
 */
interface MockDecider {
    fun canMove(shard: ModelShard, destNode: ModelNode, moves: Collection<MoveAction>): Boolean
}