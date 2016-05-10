package com.simplymeasured.elasticsearch.plugins.tempest

import org.eclipse.collections.api.RichIterable
import org.eclipse.collections.api.map.MapIterable
import org.eclipse.collections.api.set.SetIterable
import org.eclipse.collections.impl.factory.Maps
import org.eclipse.collections.impl.factory.Sets
import org.joda.time.DateTime

class BalancerState() {
    var lastOptimalBalanceFoundDateTime: DateTime = DateTime(0)
    var lastRebalanceAttemptDateTime: DateTime = DateTime(0)
    var lastStableStructuralHash: Int = 0
    var youngIndexes: SetIterable<String> = Sets.mutable.empty<String>()
    var patternMapping: MapIterable<String, RichIterable<String>> = Maps.mutable.empty()
    var clusterScore: Double = Double.MAX_VALUE
    var clusterRisk: Double = Double.MAX_VALUE
    var clusterBalanceRatio: Double = 0.0
}