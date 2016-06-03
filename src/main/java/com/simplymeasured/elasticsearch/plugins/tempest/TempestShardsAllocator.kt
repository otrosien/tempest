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

import com.simplymeasured.elasticsearch.plugins.tempest.balancer.HeuristicBalancer
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.MockDeciders
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.ShardSizeCalculator
import org.eclipse.collections.impl.factory.Sets
import org.elasticsearch.cluster.ClusterInfoService
import org.elasticsearch.cluster.InternalClusterInfoService
import org.elasticsearch.cluster.routing.MutableShardRouting
import org.elasticsearch.cluster.routing.RoutingNode
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider
import org.elasticsearch.common.component.AbstractComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.joda.time.DateTime
import org.elasticsearch.common.settings.Settings

import java.util.*

/**
 * Tempest Shards Allocator that delegates to a Heuristic Balancer
 */

class TempestShardsAllocator
    @Inject constructor(    settings: Settings,
                        val clusterInfoService: ClusterInfoService,
                        val balancerState: BalancerState) :
        AbstractComponent(settings), ShardsAllocator {

    override fun rebalance(allocation: RoutingAllocation): Boolean {
        val shardSizeCalculator = buildShardSizeCalculator(allocation)

        balancerState.lastRebalanceAttemptDateTime = DateTime.now()

        return HeuristicBalancer(
                settings,
                allocation,
                shardSizeCalculator,
                balancerState,
                Random()).rebalance();
    }

    override fun allocateUnassigned(allocation: RoutingAllocation): Boolean {
        val shardSizeCalculator = buildShardSizeCalculator(allocation)
        if (allocation.routingNodes().hasUnassignedShards()) {
            return HeuristicBalancer(
                    settings,
                    allocation,
                    shardSizeCalculator,
                    balancerState,
                    Random()).allocateUnassigned()
        }

        return false
    }

    override fun applyFailedShards(allocation: FailedRerouteAllocation) {
        /* ONLY FOR GATEWAYS */
    }

    override fun move(shardRouting: MutableShardRouting, node: RoutingNode, allocation: RoutingAllocation): Boolean {
        val shardSizeCalculator = buildShardSizeCalculator(allocation)
        return HeuristicBalancer(
                settings,
                allocation,
                shardSizeCalculator,
                balancerState,
                Random()).moveShards();
    }

    override fun applyStartedShards(allocation: StartedRerouteAllocation) {
        /* ONLY FOR GATEWAYS */
    }

    private fun buildShardSizeCalculator(allocation: RoutingAllocation): ShardSizeCalculator {
        val shardSizeCalculator = ShardSizeCalculator(settings, allocation.metaData(), clusterInfoService.clusterInfo, allocation.routingTable())
        balancerState.youngIndexes = shardSizeCalculator.youngIndexes()
        balancerState.patternMapping = shardSizeCalculator.patternMapping()
        return shardSizeCalculator
    }

}

