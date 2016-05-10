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
import org.joda.time.DateTime
import java.util.*

/**
 * Created by awhite on 4/15/16.
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

    private fun buildShardSizeCalculator(allocation: RoutingAllocation): ShardSizeCalculator {
        val shardSizeCalculator = ShardSizeCalculator(settings, allocation.metaData(), clusterInfoService.clusterInfo, allocation.routingTable())
        balancerState.youngIndexes = shardSizeCalculator.youngIndexes()
        balancerState.patternMapping = shardSizeCalculator.patternMapping()
        return shardSizeCalculator
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

    override fun moveShards(allocation: RoutingAllocation): Boolean {
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

}

