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

import com.simplymeasured.elasticsearch.plugins.tempest.actions.TempestInfoAction
import com.simplymeasured.elasticsearch.plugins.tempest.actions.TransportTempestInfoAction
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.BalancerConfiguration
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.IndexGroupPartitioner
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.ShardSizeCalculator
import com.simplymeasured.elasticsearch.plugins.tempest.handlers.TempestInfoRestHandler
import com.simplymeasured.elasticsearch.plugins.tempest.handlers.TempestRebalanceRestHandler
import org.eclipse.collections.impl.factory.Lists
import org.eclipse.collections.impl.factory.Maps
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.node.DiscoveryNodes
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.*
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.plugins.ActionPlugin
import org.elasticsearch.plugins.ClusterPlugin
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.script.ScriptService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.watcher.ResourceWatcherService
import java.util.function.Supplier

/**
 * Main configuration entry point for the Tempest Plugin Required for ES Integration
 */

class TempestPlugin(val settings: Settings) : Plugin(), ActionPlugin, ClusterPlugin {
    private lateinit var balancerConfiguration: BalancerConfiguration
    private lateinit var indexGroupPartitioner: IndexGroupPartitioner
    private lateinit var shardSizeCalculator: ShardSizeCalculator

    override fun getSettings(): MutableList<Setting<*>> {
        return Lists.mutable.of(
                 BalancerConfiguration.CONCURRENT_REBALANCE_SETTING,
                 BalancerConfiguration.EXCLUDE_GROUP_SETTING,
                 BalancerConfiguration.SEARCH_DEPTH_SETTING,
                 BalancerConfiguration.SEARCH_SCALE_FACTOR_SETTING,
                 BalancerConfiguration.BEST_NQUEUE_SIZE_SETTING,
                 BalancerConfiguration.MINIMUM_SHARD_MOVEMENT_OVERHEAD_SETTING,
                 BalancerConfiguration.MAXIMUM_ALLOWED_RISK_RATE_SETTING,
                 BalancerConfiguration.MINIMUM_NODE_SIZE_CHANGE_RATE_SETTING,
                 BalancerConfiguration.EXPUNGE_BLACKLISTED_NODES_SETTING,
                 BalancerConfiguration.SEARCH_TIME_LIMIT_SECONDS_SETTING,
                 IndexGroupPartitioner.INDEX_GROUP_PATTERN_SETTING,
                 ShardSizeCalculator.MODEL_AGE_IN_MINUTES_SETTING)
    }

    override fun createComponents(
            client: Client,
            clusterService: ClusterService,
            threadPool: ThreadPool,
            resourceWatcherService: ResourceWatcherService,
            scriptService: ScriptService,
            xContentRegistry: NamedXContentRegistry): MutableCollection<Any> {

        balancerConfiguration = BalancerConfiguration(settings, clusterService.clusterSettings)
        indexGroupPartitioner = IndexGroupPartitioner(settings, clusterService.clusterSettings)
        shardSizeCalculator = ShardSizeCalculator(settings, clusterService.clusterSettings, indexGroupPartitioner)
        return Lists.mutable.empty()
    }

    override fun getShardsAllocators(
            settings: Settings,
            clusterSettings: ClusterSettings): MutableMap<String, Supplier<ShardsAllocator>> {
        return Maps.mutable.of("tempest", Supplier { buildTempestAllocator(settings) })
    }

    private fun buildTempestAllocator(settings: Settings): ShardsAllocator {
        return TempestShardsAllocator(
                settings,
                balancerConfiguration,
                indexGroupPartitioner,
                shardSizeCalculator)
    }

    override fun getRestHandlers(
            settings: Settings,
            restController: RestController,
            clusterSettings: ClusterSettings,
            indexScopedSettings: IndexScopedSettings,
            settingsFilter: SettingsFilter,
            indexNameExpressionResolver: IndexNameExpressionResolver,
            nodesInCluster: Supplier<DiscoveryNodes>): MutableList<RestHandler> {
        return Lists.mutable.of(
                TempestInfoRestHandler(settings, restController),
                TempestRebalanceRestHandler(settings, restController))

    }

    override fun getActions(): MutableList<ActionPlugin.ActionHandler<out ActionRequest, out ActionResponse>> {
        return Lists.mutable.of(
            ActionPlugin.ActionHandler(TempestInfoAction.INSTANCE, TransportTempestInfoAction::class.java))
    }
}

