package com.simplymeasured.elasticsearch.plugins.tempest.balancer

/**
 * Created by awhite on 4/17/16.
 */
class MoveActionBatch(val moves: List<MoveAction>, val overhead: Long, val risk: Double, val score: Double) {

}