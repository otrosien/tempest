package com.simplymeasured.elasticsearch.plugins.tempest

import org.eclipse.collections.api.RichIterable
import org.eclipse.collections.api.map.MapIterable
import org.eclipse.collections.api.set.SetIterable
import org.eclipse.collections.impl.factory.Maps
import org.eclipse.collections.impl.factory.Sets
import org.joda.time.DateTime

/**
 * A shared state used by the allocator, balancer, and rest endpoint for book keeping and reporting
 */
class BalancerState() {
    // ideally this should be a data class I think but Kotlin is requiring constructor parameters for that and the
    // default no-arg constructor just makes more sense here.
    var lastOptimalBalanceFoundDateTime: DateTime = DateTime(0)
    var lastBalanceChangeDateTime: DateTime = DateTime(0)
    var lastRebalanceAttemptDateTime: DateTime = DateTime(0)
    var lastStableStructuralHash: Int = 0
    var youngIndexes: SetIterable<String> = Sets.mutable.empty<String>()
    var patternMapping: MapIterable<String, RichIterable<String>> = Maps.mutable.empty()
    var clusterScore: Double = Double.MAX_VALUE
    var clusterRisk: Double = Double.MAX_VALUE
    var clusterBalanceRatio: Double = 0.0
}