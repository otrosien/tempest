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

package com.simplymeasured.elasticsearch.plugins.tempest.balancer

import com.simplymeasured.elasticsearch.plugins.tempest.TempestConstants
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.IndexSizingGroup.*
import org.eclipse.collections.api.RichIterable
import org.eclipse.collections.api.list.MutableList
import org.eclipse.collections.api.map.MapIterable
import org.eclipse.collections.impl.factory.Lists
import org.eclipse.collections.impl.factory.Maps
import org.eclipse.collections.impl.list.mutable.CompositeFastList
import org.eclipse.collections.impl.list.mutable.FastList
import org.eclipse.collections.impl.tuple.Tuples.pair
import org.eclipse.collections.impl.utility.LazyIterate
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.cluster.routing.ShardRouting
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation
import org.elasticsearch.common.component.AbstractComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.ClusterSettings
import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.settings.Setting.Property.*
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.shard.ShardId
import org.joda.time.DateTime

/**
 * Shard Size Calculator and Estimator used for preemptive balancing
 *
 * Users define index groups by writing regexes that match index names. When the balancer asks for a shard's size
 * this class will either report the actual size of the shard or an estimate for how big the shard will get in the near
 * future.
 *
 * The estimation step only occurs if the index in question is young and if it belongs to a group with indexes that are
 * not young. Also, if the actual size of a shard is ever larger than the actual size, the estimation is ignored.
 *
 * Estimates come in two flavors, homogeneous and non-homogeneous. Homogeneous estimation occurs when the new index and
 * all non-young indexes in a group have the same number of shards. In this case the estimated size of a shard is the
 * average of all like-id shards in the group. For non-homogeneous groups, the average of all shards in the group is
 * used.
 *
 * Note, any indexes not matching a pattern are placed into a "default" group
 */
class ShardSizeCalculator
    @Inject constructor(
        settings: Settings,
        clusterSettings: ClusterSettings,
        private val indexGroupPartitioner: IndexGroupPartitioner)
    : AbstractComponent(settings) {

    companion object {
        val MODEL_AGE_IN_MINUTES_SETTING = Setting.intSetting(TempestConstants.MODEL_AGE_MINUTES, 60*12, 0, NodeScope, Dynamic)
    }

    private var modelAgeInMinutes = MODEL_AGE_IN_MINUTES_SETTING.get(settings)

    init {
        clusterSettings.addSettingsUpdateConsumer(MODEL_AGE_IN_MINUTES_SETTING, this::modelAgeInMinutes.setter)
    }

    fun buildShardSizeInfo(allocation: RoutingAllocation) : MapIterable<ShardId, ShardSizeInfo> {
        val shards = LazyIterate.concatenate(
                allocation.routingNodes().flatMap { it },
                allocation.routingNodes().unassigned())

        val indexSizingGroupMap = buildIndexGroupMap(allocation.metaData())

        return shards
                .groupBy { it.shardId() }
                .toMap()
                .keyValuesView()
                .collect { pair(it.one, it.two.collect { buildShardSizingInfo(allocation, it, indexSizingGroupMap) })}
                .toMap({ it.one }, { it.two.maxBy { it.estimatedSize } })
    }

    private fun buildShardSizingInfo(
            allocation: RoutingAllocation,
            shard: ShardRouting, indexSizingGroupMap:
            MapIterable<String, IndexSizingGroup>): ShardSizeInfo {
        return ShardSizeInfo(
                actualSize = allocation.clusterInfo().getShardSize(shard) ?: 0,
                estimatedSize = estimateShardSize(allocation, indexSizingGroupMap[shard.index().name]!!, shard))
    }

    private fun buildIndexGroupMap(metaData: MetaData): MapIterable<String, IndexSizingGroup> {
        val modelTimestampThreshold = DateTime().minusMinutes(modelAgeInMinutes)
        val indexGroups = indexGroupPartitioner.partition(metaData)
        val indexSizingGroups = FastList.newWithNValues(indexGroups.size(), {IndexSizingGroup()})

        return Maps.mutable.empty<String, IndexSizingGroup>().apply {
            metaData.forEach { indexMetaData ->
                val indexGroup = indexGroups
                        .indexOfFirst { it.contains(indexMetaData) }
                        .let { indexSizingGroups[it] }

                when {
                    modelTimestampThreshold.isAfter(indexMetaData.creationDate) -> indexGroup.addModel(indexMetaData)
                    else -> indexGroup.addYoungIndex(indexMetaData)
                }

                this.put(indexMetaData.index.name, indexGroup)
            }
        }
    }

    private fun estimateShardSize(
            routingAllocation: RoutingAllocation,
            indexSizingGroup: IndexSizingGroup,
            shardRouting: ShardRouting) : Long {

        return arrayOf(routingAllocation.clusterInfo().getShardSize(shardRouting) ?: 0,
                shardRouting.expectedShardSize,
                calculateEstimatedShardSize(
                        routingAllocation,
                        indexSizingGroup,
                        shardRouting)).max()
                ?: 0L
    }

    private fun calculateEstimatedShardSize(
            routingAllocation: RoutingAllocation,
            indexSizingGroup: IndexSizingGroup,
            shardRouting: ShardRouting): Long {

        when {
            indexSizingGroup.modelIndexes.anySatisfy { it.index == shardRouting.index() } ->
                // for older indexes we can usually trust the actual shard size but not when the shard is being initialized
                // from a dead node. In those cases we need to "guess" the size by looking at started replicas with the
                // same shard id on that index
                return Math.max(routingAllocation.clusterInfo().getShardSize(shardRouting) ?: 0,
                                routingAllocation.findLargestReplicaSize(shardRouting))
            !indexSizingGroup.hasModelIndexes() ->
                // it's possible for a newly created index to have no models (or for older versions of the index to be
                // nuked). In these cases the best guess we have is the actual shard size
                return routingAllocation.clusterInfo().getShardSize(shardRouting) ?: 0
            indexSizingGroup.isHomogeneous() ->
                // this is an ideal case where we see that the new (young) index and one or more model (old) indexes
                // look alike. So we want to use the older indexes as a "guess" to the new size. Note that we do an
                // average just in case multiple models exists (rare but useful)
                return indexSizingGroup.modelIndexes
                    .map { routingAllocation.findLargestShardSizeById(it.index.name, shardRouting.id) }
                    .average()
                    .toLong()
            else ->
                // this is a less ideal case where we see a new index with no good model to fall back on. We can still
                // make an educated guess on the shard size by averaging the size of the shards in the models
                return indexSizingGroup.modelIndexes
                    .flatMap { routingAllocation.routingTable().index(it.index).shards.values() }
                    .map { routingAllocation.findLargestShardSizeById(it.value.shardId().index.name, it.value.shardId.id) }
                    .average()
                    .toLong()
        }
    }

    fun youngIndexes(metaData: MetaData) : RichIterable<String> =
            buildIndexGroupMap(metaData)
                    .valuesView()
                    .toSet()
                    .flatCollect { it.youngIndexes }
                    .collect { it.index.name }
}


// considers both primaries and replicas in order to worst case scenario for shard size estimation
fun RoutingAllocation.findLargestShardSizeById(index: String, id: Int) : Long =
        routingTable()
                .index(index)
                .shard(id)
                .map { clusterInfo().getShardSize(it) ?: 0 }
                .max() ?: 0

fun RoutingAllocation.findLargestReplicaSize(shardRouting: ShardRouting): Long =
        findLargestShardSizeById(shardRouting.index().name, shardRouting.id)

class IndexSizingGroup {
    val modelIndexes: MutableList<IndexMetaData> = Lists.mutable.empty<IndexMetaData>()
    val youngIndexes: MutableList<IndexMetaData> = Lists.mutable.empty<IndexMetaData>()

    fun addModel(indexMetadata: IndexMetaData) {
        modelIndexes.add(indexMetadata)
    }

    fun addYoungIndex(indexMetadata: IndexMetaData) {
        youngIndexes.add(indexMetadata)
    }

    fun hasModelIndexes(): Boolean {
        return modelIndexes.isNotEmpty()
    }

    fun isHomogeneous(): Boolean {
        val allIndexes = allIndexes()

        if (allIndexes.isEmpty) { return false; }
        val value = allIndexes.first.numberOfShards
        return allIndexes.all { it.numberOfShards == value }
    }

    fun allIndexes(): CompositeFastList<IndexMetaData> {
        val allIndexes = CompositeFastList<IndexMetaData>()
        allIndexes.addComposited(modelIndexes)
        allIndexes.addComposited(youngIndexes)
        return allIndexes
    }

    data class ShardSizeInfo(val actualSize: Long, val estimatedSize: Long)
}