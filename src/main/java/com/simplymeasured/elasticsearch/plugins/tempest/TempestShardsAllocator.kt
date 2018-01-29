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

import com.simplymeasured.elasticsearch.plugins.tempest.balancer.*
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.model.ModelNode
import org.eclipse.collections.api.map.MapIterable
import org.eclipse.collections.impl.factory.Maps
import org.elasticsearch.cluster.ClusterInfoService
import org.elasticsearch.cluster.ClusterService
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator
import org.elasticsearch.common.component.AbstractComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.inject.Singleton
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.settings.NodeSettingsService
import org.joda.time.DateTime
import java.util.*

/**
 * Tempest Shards Allocator that delegates to a Heuristic Balancer
 */

@Singleton
class TempestShardsAllocator
    @Inject constructor(settings: Settings,
                        settingsService: NodeSettingsService,
                        val clusterService: ClusterService,
                        val clusterInfoService: ClusterInfoService,
                        val indexGroupPartitioner: IndexGroupPartitioner,
                        val shardSizeCalculator: ShardSizeCalculator) :
        AbstractComponent(settings), ShardsAllocator {

    var balancerConfiguration: BalancerConfiguration = BalancerConfiguration(settings)
    var effectiveSettings: Settings = settings
    var lastRebalanceAttemptDateTime: DateTime = DateTime(0)
    var lastBalanceChangeDateTime: DateTime = DateTime(0)
    var lastOptimalBalanceFoundDateTime: DateTime = DateTime(0)
    var lastClusterBalanceScore: Double = 0.0
    var lastNodeGroupScores: MapIterable<String, MapIterable<String, Double>> = Maps.immutable.empty<String, MapIterable<String, Double>>()
    var status: String = "unknown"
    var random: Random = Random()

    init {
        updateComponentSettings(settings)
        settingsService.addListener { newSettings ->
            balancerConfiguration = BalancerConfiguration(newSettings, balancerConfiguration)
            effectiveSettings = Settings.builder().put(effectiveSettings).put(newSettings).build()
            updateComponentSettings(effectiveSettings)
        }
    }

    private fun updateComponentSettings(newSettings: Settings) {
        indexGroupPartitioner.indexGroupPatternSetting = newSettings.get(TempestConstants.GROUPING_PATTERNS, ".*")
        shardSizeCalculator.modelAgeInMinutes = newSettings.getAsInt(TempestConstants.MODEL_AGE_MINUTES, 60 * 12)
    }

    override fun rebalance(allocation: RoutingAllocation): Boolean {
        lastRebalanceAttemptDateTime = DateTime()

        return buildBalancer(allocation).run {
            updateScoreStats()
            this.rebalance().let { updateStatus(it) }
        }
    }

    override fun allocateUnassigned(allocation: RoutingAllocation): Boolean {
        return if (allocation.routingNodes().hasUnassignedShards()) {
            buildBalancer(allocation).run {
                updateScoreStats()
                this.allocateUnassigned().let { updateStatus(it) }
            }
        } else false
    }


    override fun applyFailedShards(allocation: FailedRerouteAllocation) {
        /* ONLY FOR GATEWAYS */
    }

    override fun moveShards(allocation: RoutingAllocation): Boolean {
        return buildBalancer(allocation).run {
            updateScoreStats()
            this.moveShards().let { updateStatus(it) }
        }
    }

    private fun updateStatus(balanceDecision: BalanceDecision): Boolean {
        when (balanceDecision) {
            BalanceDecision.BALANCING -> {
                lastBalanceChangeDateTime = DateTime()
                status = "balancing"
                return true
            }

            BalanceDecision.ON_HOLD -> {
                status = "on hold"
                return false
            }

            BalanceDecision.BALANCED -> {
                lastOptimalBalanceFoundDateTime = DateTime()
                status = "balanced"
                return false
            }
            BalanceDecision.NO_OP -> {
                return false
            }
        }
    }

    private fun buildBalancer(allocation: RoutingAllocation): HeuristicBalancer = HeuristicBalancer(
            settings = effectiveSettings,
            allocation = allocation,
            shardSizeCalculator = shardSizeCalculator,
            balancerConfiguration = balancerConfiguration,
            random = random)

    private fun HeuristicBalancer.updateScoreStats() {
        lastClusterBalanceScore = this.baseModelCluster.calculateBalanceScore()
        lastNodeGroupScores = this.baseModelCluster.modelNodes
                .toMap({ it.backingNode.node().hostName }, { buildScoreGroupSummary(it) })
    }

    private fun buildScoreGroupSummary(modelNode: ModelNode): MapIterable<String, Double> {
        return modelNode.shardManager.shardScoreGroupDetails
                .keyValuesView()
                .select { it.two.balanceScore != 0.0 || it.two.shards.notEmpty() }
                .toMap( {"${it.one.index} ${if (it.one.includesPrimaries) "p" else ""}${if (it.one.includesReplicas) "r" else ""}"}, {it.two.balanceScore})
    }

    override fun applyStartedShards(allocation: StartedRerouteAllocation) {
        /* ONLY FOR GATEWAYS */
    }
}



