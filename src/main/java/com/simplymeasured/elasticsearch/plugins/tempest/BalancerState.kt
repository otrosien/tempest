/*
 * The MIT License (MIT)
 * Copyright (c) 2016 DataRank, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 *
 */

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