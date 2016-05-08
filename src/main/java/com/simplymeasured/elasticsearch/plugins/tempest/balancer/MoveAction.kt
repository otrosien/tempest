package com.simplymeasured.elasticsearch.plugins.tempest.balancer

data class MoveAction(val sourceNode: ModelNode, val shard: ModelShard, val destNode: ModelNode) {

}