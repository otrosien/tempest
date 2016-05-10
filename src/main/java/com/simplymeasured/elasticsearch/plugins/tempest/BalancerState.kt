package com.simplymeasured.elasticsearch.plugins.tempest

import org.joda.time.DateTime

class BalancerState() {
    var lastFailedRebalanceTimestamp: DateTime = DateTime(0)
    var lastStableStructuralHash: Int = 0
}