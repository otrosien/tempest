package com.simplymeasured.elasticsearch.plugins.tempest.balancer

/**
 * Simulated Move that holds the source and destination model node and shard
 */
data class MoveAction(val sourceNode: ModelNode, val shard: ModelShard, val destNode: ModelNode) {

}