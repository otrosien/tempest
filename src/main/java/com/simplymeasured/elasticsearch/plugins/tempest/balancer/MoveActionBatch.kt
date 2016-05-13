package com.simplymeasured.elasticsearch.plugins.tempest.balancer

/**
 * Capture the moves for a given batch plus some cluster stats assuming the batch is applied
 */
class MoveActionBatch(val moves: List<MoveAction>, val overhead: Long, val risk: Double, val score: Double) {

}