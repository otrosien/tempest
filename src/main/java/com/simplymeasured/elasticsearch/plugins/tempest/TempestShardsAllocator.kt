package com.simplymeasured.elasticsearch.plugins.tempest

import com.simplymeasured.elasticsearch.plugins.tempest.balancer.HeuristicBalancer
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.MockDeciders
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.ShardSizeCalculator
import org.eclipse.collections.impl.factory.Sets
import org.elasticsearch.cluster.ClusterInfoService
import org.elasticsearch.cluster.InternalClusterInfoService
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider
import org.elasticsearch.common.component.AbstractComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import java.util.*

/**
 * Created by awhite on 4/15/16.
 */

class TempestShardsAllocator
    @Inject constructor(settings: Settings, val clusterInfoService: ClusterInfoService) :
        AbstractComponent(settings), ShardsAllocator {

    val balancerState = BalancerState()

    override fun rebalance(allocation: RoutingAllocation): Boolean {
        val shardSizeCalculator = ShardSizeCalculator(settings, allocation.metaData(), clusterInfoService.clusterInfo, allocation.routingTable())
        return HeuristicBalancer(
                settings,
                allocation,
                shardSizeCalculator,
                balancerState,
                Random()).rebalance();
    }

    override fun allocateUnassigned(allocation: RoutingAllocation): Boolean {
        val shardSizeCalculator = ShardSizeCalculator(settings, allocation.metaData(), clusterInfoService.clusterInfo, allocation.routingTable())
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

    override fun moveShards(allocation: RoutingAllocation): Boolean {
        val shardSizeCalculator = ShardSizeCalculator(settings, allocation.metaData(), clusterInfoService.clusterInfo, allocation.routingTable())
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

}

