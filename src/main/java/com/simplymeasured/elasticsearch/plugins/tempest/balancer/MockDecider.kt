package com.simplymeasured.elasticsearch.plugins.tempest.balancer

interface MockDecider {
    fun canMove(shard: ModelShard, destNode: ModelNode, moves: Collection<MoveAction>): Boolean
}