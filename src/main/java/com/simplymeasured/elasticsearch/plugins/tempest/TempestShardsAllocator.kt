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

import com.simplymeasured.elasticsearch.plugins.tempest.balancer.BalancerConfiguration
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.HeuristicBalancer
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.MockDeciders
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.ShardSizeCalculator
import org.eclipse.collections.impl.factory.Sets
import org.elasticsearch.cluster.*
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider
import org.elasticsearch.common.component.AbstractComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.inject.Singleton
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.settings.IndexSettingsService
import org.elasticsearch.node.settings.NodeSettingsService
import org.joda.time.DateTime
import java.util.*

/**
 * Tempest Shards Allocator that delegates to a Heuristic Balancer
 */

@Singleton
class TempestShardsAllocator
    @Inject constructor(    settings: Settings,
                            settingsService: NodeSettingsService,
                        val clusterService: ClusterService,
                        val clusterInfoService: ClusterInfoService) :
        AbstractComponent(settings), ShardsAllocator {

    var balancerConfiguration: BalancerConfiguration = BalancerConfiguration(settings)
    var effectiveSettings: Settings = settings
    var lastRebalanceAttemptDateTime: DateTime = DateTime(0)
    var lastBalanceChangeDateTime: DateTime = DateTime(0)
    var lastOptimalBalanceFoundDateTime: DateTime = DateTime(0)
    var  status: String = "unknown"

    init {
        settingsService.addListener({
            settings -> balancerConfiguration = BalancerConfiguration(settings, balancerConfiguration)
                        effectiveSettings = Settings.builder().put(effectiveSettings).put(settings).build()
        })
    }

    override fun rebalance(allocation: RoutingAllocation): Boolean {
        val shardSizeCalculator = buildShardSizeCalculator(allocation)

        lastRebalanceAttemptDateTime = DateTime()

        return HeuristicBalancer(
                effectiveSettings,
                allocation,
                shardSizeCalculator,
                balancerConfiguration,
                Random()).rebalance().apply {
            if (this == true) {
                lastBalanceChangeDateTime = DateTime()
                status = "balancing"
            }
            else {
                lastOptimalBalanceFoundDateTime = DateTime()
                status = "balanced"
            }
        }
    }

    override fun allocateUnassigned(allocation: RoutingAllocation): Boolean {
        val shardSizeCalculator = buildShardSizeCalculator(allocation)

        if (allocation.routingNodes().hasUnassignedShards()) {
            return HeuristicBalancer(
                    effectiveSettings,
                    allocation,
                    shardSizeCalculator,
                    balancerConfiguration,
                    Random()).allocateUnassigned().apply {
                if (this == true) {
                    status = "allocating"
                }
            }
        }

        return false
    }

    override fun applyFailedShards(allocation: FailedRerouteAllocation) {
        /* ONLY FOR GATEWAYS */
    }

    override fun moveShards(allocation: RoutingAllocation): Boolean {
        val shardSizeCalculator = buildShardSizeCalculator(allocation)

        return HeuristicBalancer(
                effectiveSettings,
                allocation,
                shardSizeCalculator,
                balancerConfiguration,
                Random()).moveShards().apply {
            if (this == true) {
                status = "moving"
            }
        }
    }

    override fun applyStartedShards(allocation: StartedRerouteAllocation) {
        /* ONLY FOR GATEWAYS */
    }

    internal fun buildShardSizeCalculator(allocation: RoutingAllocation): ShardSizeCalculator =
            ShardSizeCalculator(
                    effectiveSettings,
                    allocation.metaData(),
                    allocation.clusterInfo(),
                    allocation.routingTable())
}



