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

import com.carrotsearch.randomizedtesting.RandomizedContext
import com.carrotsearch.randomizedtesting.RandomizedTest.getRandom
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.*
import org.eclipse.collections.api.map.MutableMap
import org.eclipse.collections.impl.factory.Lists
import org.eclipse.collections.impl.factory.Maps
import org.eclipse.collections.impl.factory.Sets
import org.eclipse.collections.impl.lazy.CompositeIterable
import org.elasticsearch.Version
import org.elasticsearch.cluster.*
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.cluster.node.DiscoveryNodes
import org.elasticsearch.cluster.routing.RoutingTable
import org.elasticsearch.cluster.routing.ShardRouting
import org.elasticsearch.cluster.routing.ShardRoutingState
import org.elasticsearch.cluster.routing.allocation.AllocationService
import org.elasticsearch.common.collect.ImmutableOpenMap
import org.elasticsearch.common.settings.ClusterSettings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.test.gateway.NoopGatewayAllocator
import org.joda.time.DateTime
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import org.mockito.Mockito.mock
import java.security.AccessController
import java.security.PrivilegedAction

/**
 * Created by awhite on 5/1/16.
 */
@TimeoutSuite(millis = 30 * 60 * 1000)
class TempestShardsAllocatorTests : ESAllocationTestCase() {
    private val tempestSettings = Lists.mutable.of(
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

    private val allSettings = CompositeIterable.with(tempestSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)

    @Before
    fun setup() {
        AccessController.doPrivileged(PrivilegedAction { System.setSecurityManager(null) })
    }

    @Test
    fun testBasicBalance() {
        // can be reproduced with -Dtests.seed=<seed-id>
        println("seed = ${RandomizedContext.current().runnerSeedAsString}")

        val settings = Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", 8)
                .build()

        val shardSizes = Maps.mutable.empty<String, Long>()
        val testClusterInfo = ClusterInfo(
                ImmutableOpenMap.of<String, DiskUsage>(),
                ImmutableOpenMap.of<String, DiskUsage>(),
                ImmutableOpenMap.of<String, Long>(),
                ImmutableOpenMap.of<ShardRouting, String>())
        val mockClusterInfoService = mock(ClusterInfoService::class.java)
        val mockClusterSettings = ClusterSettings(settings, Sets.mutable.ofAll(allSettings))
        val indexGroupPartitioner = IndexGroupPartitioner(settings, mockClusterSettings)
        val shardSizeCalculator = ShardSizeCalculator(settings, mockClusterSettings, indexGroupPartitioner)
        val balancerConfiguration = BalancerConfiguration(settings, mockClusterSettings)
        Mockito.`when`(mockClusterInfoService.clusterInfo).thenReturn(testClusterInfo)

        val tempestShardsAllocator = TempestShardsAllocator(
                settings = settings,
                balancerConfiguration = balancerConfiguration,
                indexGroupPartitioner = indexGroupPartitioner,
                shardSizeCalculator = shardSizeCalculator)

        val strategy = MockAllocationService(
                settings,
                randomAllocationDeciders(settings, mockClusterSettings, getRandom()),
                NoopGatewayAllocator.INSTANCE,
                tempestShardsAllocator,
                mockClusterInfoService)

        var (routingTable, clusterState) = createCluster(strategy)
        println(tempestShardsAllocator.lastClusterBalanceScore)

        routingTable.allShards().forEach { assertEquals(ShardRoutingState.STARTED, it.state()) }
        assignRandomShardSizes(routingTable, shardSizes)
        println(tempestShardsAllocator.lastClusterBalanceScore)

        routingTable = strategy.reroute(clusterState, "reroute").routingTable()
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build()

        while (clusterState.routingNodes.shardsWithState(ShardRoutingState.INITIALIZING).isEmpty().not()) {
            routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes.shardsWithState(ShardRoutingState.INITIALIZING)).routingTable()
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build()
            println(tempestShardsAllocator.lastClusterBalanceScore)
        }
    }

    protected fun createCluster(strategy: MockAllocationService): Pair<RoutingTable, ClusterState> {

        val indexes = (1..(5 + randomInt(5))).map { createRandomIndex(Integer.toHexString(it)) }
        val metaData = MetaData.builder().apply { indexes.forEach { this.put(it) } }.build()
        val routingTable = RoutingTable.builder().apply { indexes.forEach { this.addAsNew(metaData.index(it.index())) } }.build()

        val clusterState = ClusterState.builder(
                org.elasticsearch.cluster.ClusterName.DEFAULT)
                .metaData(metaData)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().apply { (1..(3 + randomInt(100))).forEach { this.add(newNode("node${it}")) } })
                .build()

        return startupCluster(routingTable, clusterState, strategy)
    }

    fun createRandomIndex(id: String): IndexMetaData.Builder {
        return IndexMetaData
                .builder("index-${id}")
                .settings(Settings.builder()
                        .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetaData.SETTING_CREATION_DATE, DateTime().millis))
                .numberOfShards(5 + randomInt(100))
                .numberOfReplicas( randomInt(2))
    }

    fun startupCluster(routingTable: RoutingTable, clusterState: ClusterState, strategy: AllocationService) : Pair<RoutingTable, ClusterState> {

        var resultRoutingTable: RoutingTable = routingTable
        var resultClusterState: ClusterState = clusterState

        for (attempt in 1..100) {
            resultRoutingTable = strategy.reroute(resultClusterState, "reroute").routingTable()
            resultClusterState = ClusterState.builder(resultClusterState).routingTable(resultRoutingTable).build()

            resultRoutingTable = strategy.applyStartedShards(resultClusterState, resultClusterState.routingNodes.shardsWithState(ShardRoutingState.INITIALIZING)).routingTable()
            resultClusterState = ClusterState.builder(resultClusterState).routingTable(resultRoutingTable).build()

            if (resultRoutingTable.allShards().all { it.state() == ShardRoutingState.STARTED }) {
                return Pair(resultRoutingTable, resultClusterState)
            }
        }

        fail()
        throw RuntimeException()
    }

    private fun assignRandomShardSizes(routingTable: RoutingTable, shardSizes: MutableMap<String, Long>) {
        val shardSizeMap = Maps.mutable.empty<ShardId, Long>()

        for (shard in routingTable.allShards()) {
            val shardSize = shardSizeMap.getIfAbsentPut(shard.shardId(), { Math.exp(20.0 + randomDouble() * 5.0).toLong() })
            shardSizes.put(shardIdentifierFromRouting(shard), shardSize)
        }
    }

    fun shardIdentifierFromRouting(shardRouting: ShardRouting): String {
        return shardRouting.shardId().toString() + "[" + (if (shardRouting.primary()) "p" else "r") + "]"
    }
}