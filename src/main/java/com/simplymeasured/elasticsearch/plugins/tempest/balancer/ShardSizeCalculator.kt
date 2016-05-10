package com.simplymeasured.elasticsearch.plugins.tempest.balancer

import org.eclipse.collections.impl.factory.Lists
import org.eclipse.collections.impl.factory.Maps
import org.eclipse.collections.impl.factory.Sets
import org.eclipse.collections.impl.list.mutable.CompositeFastList
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
 * Created by awhite on 5/9/16.
 */
class ShardSizeCalculator(settings: Settings, metadata: MetaData, private val clusterInfo: ClusterInfo, private val routingTable: RoutingTable) : AbstractComponent(settings) {
    private val indexNameGroupMap = Maps.mutable.empty<String, IndexGroup>()
    private val estimatedShardSizes = Maps.mutable.empty<ShardRouting, Long>()
    private val defaultGroup = IndexGroup()
    private val allGroup = IndexGroup()
    private val youngIndexes = Sets.mutable.empty<String>()

    init {
        val indexPatterns = settings.get("tempest.balancer.groupingPatterns", "").split(",").map { safeCompile(it) }.filterNotNull()
        val modelAgeInMinutes = settings.getAsInt("tempest.balancer.modelAgeMinutes", 60*12)
        val modelTimestampThreshold = DateTime().millis - modelAgeInMinutes * 60 * 1000
        val indexPatternGroupMap = Maps.mutable.empty<Pattern, IndexGroup>()

        for (indexMetadata in metadata) {
            val matchedPattern = indexPatterns.firstOrNull { it.matcher(indexMetadata.index).matches() }
            val indexGroup = if (matchedPattern == null) defaultGroup else indexPatternGroupMap.getIfAbsentPut(matchedPattern, { IndexGroup() })
            indexNameGroupMap.put(indexMetadata.index, indexGroup)

            if (indexMetadata.creationDate < modelTimestampThreshold) {
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
        return estimatedShardSizes.getIfAbsentPut(shardRouting, { Math.max(clusterInfo.getShardSize(shardRouting, 0), calculateEstimatedShardSize(shardRouting)) })
    }

    fun actualShardSize(shardRouting: ShardRouting) : Long {
        return clusterInfo.getShardSize(shardRouting, 0)
    }

    private fun calculateEstimatedShardSize(shardRouting: ShardRouting): Long {
        if (!youngIndexes.contains(shardRouting.index())) { return actualShardSize(shardRouting); }

        val indexGroup = indexNameGroupMap.getIfAbsent(shardRouting.index, { if (defaultGroup.hasModelIndexes()) defaultGroup else allGroup })
        if (indexGroup.hasModelIndexes()) {
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

        return 0
    }

    private fun findPrimaryShardById(it: IndexMetaData, id: Int) = routingTable.index(it.index).shard(id).primaryShard()
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
        val allIndexes = CompositeFastList<IndexMetaData>()
        allIndexes.addComposited(modelIndexes)
        allIndexes.addComposited(youngIndexes)

        if (allIndexes.isEmpty) { return false; }
        val value = allIndexes.first.totalNumberOfShards
        return allIndexes.all { it.totalNumberOfShards == value }
    }
}