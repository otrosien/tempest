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

import org.eclipse.collections.api.RichIterable
import org.eclipse.collections.api.list.ListIterable
import org.eclipse.collections.api.map.MapIterable
import org.eclipse.collections.api.set.SetIterable
import org.eclipse.collections.impl.factory.Lists
import org.eclipse.collections.impl.factory.Maps
import org.eclipse.collections.impl.factory.Sets
import org.eclipse.collections.impl.list.mutable.CompositeFastList
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import org.eclipse.collections.impl.tuple.Tuples
import org.eclipse.collections.impl.utility.ArrayIterate
import org.eclipse.collections.impl.utility.LazyIterate
import org.elasticsearch.cluster.ClusterInfo
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.cluster.routing.RoutingNodes
import org.elasticsearch.cluster.routing.RoutingTable
import org.elasticsearch.cluster.routing.ShardRouting
import org.elasticsearch.common.component.AbstractComponent
import org.elasticsearch.common.settings.Settings
import org.joda.time.DateTime
import java.util.regex.Pattern
import java.util.regex.PatternSyntaxException

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
class ShardSizeCalculator(settings: Settings, metadata: MetaData, private val clusterInfo: ClusterInfo, private val routingTable: RoutingTable) : AbstractComponent(settings) {
    private val indexNameGroupMap = Maps.mutable.empty<String, IndexGroup>()
    private val estimatedShardSizes = Maps.mutable.empty<ShardRouting, Long>()
    private val defaultGroup = IndexGroup()
    private val allGroup = IndexGroup()
    private val youngIndexes = Sets.mutable.empty<String>()
    val indexPatternGroupMap = Maps.mutable.empty<Pattern, IndexGroup>()

    init {
        // commas aren't perfect here since they can legally be defined in regexes but it seems reasonable for now;
        // perhaps there is a more generic way to define groups
        val indexPatterns = settings.get("tempest.balancer.groupingPatterns", "").split(",").map { safeCompile(it) }.filterNotNull()
        val modelAgeInMinutes = settings.getAsInt("tempest.balancer.modelAgeMinutes", 60*12)
        val modelTimestampThreshold = DateTime().minusMinutes(modelAgeInMinutes)

        for (indexMetadata in metadata) {
            val matchedPattern = indexPatterns.firstOrNull { it.matcher(indexMetadata.index).matches() }
            val indexGroup = if (matchedPattern == null) defaultGroup else indexPatternGroupMap.getIfAbsentPut(matchedPattern, { IndexGroup() })
            indexNameGroupMap.put(indexMetadata.index, indexGroup)

            if (modelTimestampThreshold.isAfter(indexMetadata.creationDate)) {
                indexGroup.addModel(indexMetadata)
                allGroup.addModel(indexMetadata)
            }
            else {
                youngIndexes.add(indexMetadata.index)
                indexGroup.addYoungIndex(indexMetadata)
                allGroup.addYoungIndex(indexMetadata)
            }
        }
    }

    private fun safeCompile(it: String): Pattern? {
        try {
            return Pattern.compile(it)
        } catch(e: PatternSyntaxException) {
            logger.warn("failed to compile group pattern ${it}");
            return null;
        }
    }

    fun estimateShardSize(shardRouting: ShardRouting) : Long {
        return estimatedShardSizes.getIfAbsentPut(shardRouting, {
            arrayOf(actualShardSize(shardRouting),
                    shardRouting.expectedShardSize,
                    calculateEstimatedShardSize(shardRouting)).max() })
    }

    fun actualShardSize(shardRouting: ShardRouting) : Long {
        return clusterInfo.getShardSize(shardRouting, 0)
    }

    private fun calculateEstimatedShardSize(shardRouting: ShardRouting): Long {
        if (!youngIndexes.contains(shardRouting.index())) { return actualShardSize(shardRouting) }

        val indexGroup = indexNameGroupMap.get(shardRouting.index)
        if (indexGroup == null || !indexGroup.hasModelIndexes()) { return actualShardSize(shardRouting) }

        if (indexGroup.isHomogeneous()) {
            return indexGroup.modelIndexes
                    .map { findPrimaryShardById(it, shardRouting.id) }
                    .map { clusterInfo.getShardSize(it, 0) }
                    .average()
                    .toLong()
        }

        return indexGroup.modelIndexes
                .flatMap { routingTable.index(it.index).shards.values() }
                .map { it.value.primaryShard() }
                .map { clusterInfo.getShardSize(it, 0) }
                .average()
                .toLong()
    }

    private fun findPrimaryShardById(it: IndexMetaData, id: Int) = routingTable.index(it.index).shard(id).primaryShard()

    fun patternMapping() : MapIterable<String, RichIterable<String>> = indexPatternGroupMap
            .keyValuesView()
            .collect { Tuples.pair(it.one.toString(), it.two.allIndexes().collect { it.index }) }
            .toMap({it.one}, {it.two as RichIterable<String>}) // cast needed for generics bug
            .apply { put("*", defaultGroup.allIndexes().collect {it.index }) }

    fun youngIndexes() : SetIterable<String> = youngIndexes
}

class IndexGroup() {
    val modelIndexes = Lists.mutable.empty<IndexMetaData>()
    val youngIndexes = Lists.mutable.empty<IndexMetaData>()

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
}