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
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider
import org.elasticsearch.common.settings.ClusterSettings
import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.settings.Setting.*
import org.elasticsearch.common.settings.Setting.Property.Dynamic
import org.elasticsearch.common.settings.Setting.Property.NodeScope
import org.elasticsearch.common.settings.Settings

/**
 * Wrapper class for handling and providing defaults to relevant settings
 */
class BalancerConfiguration(
        settings: Settings,
        clusterSettings: ClusterSettings) {

    var concurrentRebalance: Int = CONCURRENT_REBALANCE_SETTING.get(settings)
    var excludeGroup: Settings = EXCLUDE_GROUP_SETTING.get(settings)
    var searchDepth: Int = SEARCH_DEPTH_SETTING.get(settings)
    var searchScaleFactor: Int = SEARCH_SCALE_FACTOR_SETTING.get(settings)
    var bestNQueueSize: Int = BEST_NQUEUE_SIZE_SETTING.get(settings)
    var minimumShardMovementOverhead: Long = MINIMUM_SHARD_MOVEMENT_OVERHEAD_SETTING.get(settings)
    var maximumAllowedRiskRate: Double = MAXIMUM_ALLOWED_RISK_RATE_SETTING.get(settings)
    var minimumNodeSizeChangeRate: Double = MINIMUM_NODE_SIZE_CHANGE_RATE_SETTING.get(settings)
    var expungeBlacklistedNodes: Boolean = EXPUNGE_BLACKLISTED_NODES_SETTING.get(settings)
    var searchTimeLimitSeconds: Int = SEARCH_TIME_LIMIT_SECONDS_SETTING.get(settings)

    init {
        clusterSettings.addSettingsUpdateConsumer(CONCURRENT_REBALANCE_SETTING, this::concurrentRebalance.setter)
        clusterSettings.addSettingsUpdateConsumer(EXCLUDE_GROUP_SETTING, this::excludeGroup.setter)
        clusterSettings.addSettingsUpdateConsumer(SEARCH_DEPTH_SETTING, this::searchDepth.setter)
        clusterSettings.addSettingsUpdateConsumer(SEARCH_SCALE_FACTOR_SETTING, this::searchScaleFactor.setter)
        clusterSettings.addSettingsUpdateConsumer(BEST_NQUEUE_SIZE_SETTING, this::bestNQueueSize.setter)
        clusterSettings.addSettingsUpdateConsumer(MINIMUM_SHARD_MOVEMENT_OVERHEAD_SETTING, this::minimumShardMovementOverhead.setter)
        clusterSettings.addSettingsUpdateConsumer(MAXIMUM_ALLOWED_RISK_RATE_SETTING, this::maximumAllowedRiskRate.setter)
        clusterSettings.addSettingsUpdateConsumer(MINIMUM_NODE_SIZE_CHANGE_RATE_SETTING, this::minimumNodeSizeChangeRate.setter)
        clusterSettings.addSettingsUpdateConsumer(EXPUNGE_BLACKLISTED_NODES_SETTING, this::expungeBlacklistedNodes.setter)
        clusterSettings.addSettingsUpdateConsumer(SEARCH_TIME_LIMIT_SECONDS_SETTING, this::searchTimeLimitSeconds.setter)
    }

    companion object {
        val CONCURRENT_REBALANCE_SETTING: Setting<Int> = CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING
        val EXCLUDE_GROUP_SETTING: Setting<Settings> = FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING
        val SEARCH_DEPTH_SETTING: Setting<Int> = intSetting(TempestConstants.SEARCH_DEPTH, 5, 1, Dynamic, NodeScope)
        val SEARCH_SCALE_FACTOR_SETTING: Setting<Int> = intSetting(TempestConstants.SEARCH_SCALE_FACTOR, 1000, 1, Dynamic, NodeScope)
        val BEST_NQUEUE_SIZE_SETTING: Setting<Int> = intSetting(TempestConstants.SEARCH_QUEUE_SIZE, 10, 1, Dynamic, NodeScope)
        val MINIMUM_SHARD_MOVEMENT_OVERHEAD_SETTING: Setting<Long> = longSetting(TempestConstants.MINIMUM_SHARD_MOVEMENT_OVERHEAD, 100000000, 0, Dynamic, NodeScope)
        val MAXIMUM_ALLOWED_RISK_RATE_SETTING: Setting<Double> = doubleSetting(TempestConstants.MAXIMUM_ALLOWED_RISK_RATE, 1.25, 1.00, Dynamic, NodeScope)
        val MINIMUM_NODE_SIZE_CHANGE_RATE_SETTING: Setting<Double> = doubleSetting(TempestConstants.MINIMUM_NODE_SIZE_CHANGE_RATE, 0.25, 0.00, Dynamic, NodeScope)
        val EXPUNGE_BLACKLISTED_NODES_SETTING: Setting<Boolean> = boolSetting(TempestConstants.EXPUNGE_BLACKLISTED_NODES, false, Dynamic, NodeScope)
        val SEARCH_TIME_LIMIT_SECONDS_SETTING: Setting<Int> = intSetting(TempestConstants.MAXIMUM_SEARCH_TIME_SECONDS, 5, 1, Dynamic, NodeScope)
    }
}