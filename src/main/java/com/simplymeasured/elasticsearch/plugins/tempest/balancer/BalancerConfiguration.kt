/*
 * The MIT License (MIT)
 * Copyright (c) 2017 DataRank, Inc.
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

package com.simplymeasured.elasticsearch.plugins.tempest.balancer

import com.simplymeasured.elasticsearch.plugins.tempest.TempestConstants
import org.elasticsearch.cluster.node.DiscoveryNodeFilters
import org.elasticsearch.cluster.routing.RoutingNode
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider
import org.elasticsearch.common.settings.Settings

/**
 * Wrapper class for handling and providing defaults to relavent settings
 */
class BalancerConfiguration(val concurrentRebalanceSetting: Int,
                            val searchDepthSetting: Int,
                            val searchScaleFactor: Int,
                            val bestNQueueSize: Int,
                            val minimumShardMovementOverhead: Long,
                            val maximumAllowedRiskRate: Double,
                            val minimumNodeSizeChangeRate: Double,
                            val expungeBlacklistedNodes: Boolean,
                            val clusterExcludeFilter: (RoutingNode) -> Boolean,
                            val searchTimeLimitSeconds: Long) {

    constructor(settings: Settings) : this(
            concurrentRebalanceSetting = settings.getAsInt(CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE, 4)
                                                 .let { if (it == -1) 4 else it },
            searchDepthSetting = settings.getAsInt(TempestConstants.SEARCH_DEPTH, 5),
            searchScaleFactor = settings.getAsInt(TempestConstants.SEARCH_SCALE_FACTOR, 1000),
            bestNQueueSize = settings.getAsInt(TempestConstants.SEARCH_QUEUE_SIZE, 10),
            minimumShardMovementOverhead = settings.getAsLong(TempestConstants.MINIMUM_SHARD_MOVEMENT_OVERHEAD, 100000000),
            maximumAllowedRiskRate = settings.getAsDouble(TempestConstants.MAXIMUM_ALLOWED_RISK_RATE, 1.25),
            minimumNodeSizeChangeRate = settings.getAsDouble(TempestConstants.MINIMUM_NODE_SIZE_CHANGE_RATE, 0.25),
            expungeBlacklistedNodes = settings.getAsBoolean(TempestConstants.EXPUNGE_BLACKLISTED_NODES, false),
            clusterExcludeFilter = buildBlacklistFilter(settings, { false }),
            searchTimeLimitSeconds = settings.getAsLong(TempestConstants.MAXIMUM_SEARCH_TIME_SECONDS, 5))

    constructor(settings: Settings, other: BalancerConfiguration) : this(
            concurrentRebalanceSetting = settings.getAsInt(CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE, other.concurrentRebalanceSetting),
            searchDepthSetting = settings.getAsInt(TempestConstants.SEARCH_DEPTH, other.searchDepthSetting),
            searchScaleFactor = settings.getAsInt(TempestConstants.SEARCH_SCALE_FACTOR, other.searchScaleFactor),
            bestNQueueSize = settings.getAsInt(TempestConstants.SEARCH_QUEUE_SIZE, 10),
            minimumShardMovementOverhead = settings.getAsLong(TempestConstants.MINIMUM_SHARD_MOVEMENT_OVERHEAD, other.minimumShardMovementOverhead),
            maximumAllowedRiskRate = settings.getAsDouble(TempestConstants.MAXIMUM_ALLOWED_RISK_RATE, other.maximumAllowedRiskRate),
            minimumNodeSizeChangeRate = settings.getAsDouble(TempestConstants.MINIMUM_NODE_SIZE_CHANGE_RATE, other.minimumNodeSizeChangeRate),
            expungeBlacklistedNodes = settings.getAsBoolean(TempestConstants.EXPUNGE_BLACKLISTED_NODES, other.expungeBlacklistedNodes),
            clusterExcludeFilter = buildBlacklistFilter(settings, other.clusterExcludeFilter),
            searchTimeLimitSeconds = settings.getAsLong(TempestConstants.MAXIMUM_SEARCH_TIME_SECONDS, other.searchTimeLimitSeconds))
}

private fun buildBlacklistFilter(settings: Settings, defaultFilter: (RoutingNode) -> Boolean): (RoutingNode) -> Boolean =
        settings.getByPrefix(FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP).getAsMap()
                .let {
                    if (it.isEmpty()) defaultFilter
                    else DiscoveryNodeFilters
                            .buildFromKeyValue(DiscoveryNodeFilters.OpType.OR, it)
                            .let {
                                return if (it == null) return { false }
                                       else { routingNode: RoutingNode -> it.match(routingNode.node()) }
                            }
                }