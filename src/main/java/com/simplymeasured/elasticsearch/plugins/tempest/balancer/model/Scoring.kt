/*
 * The MIT License (MIT)
 * Copyright (c) 2018 DataRank, Inc.
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

package com.simplymeasured.elasticsearch.plugins.tempest.balancer.model

import com.simplymeasured.elasticsearch.plugins.tempest.balancer.IndexSizingGroup
import org.eclipse.collections.api.RichIterable
import org.eclipse.collections.api.list.ListIterable
import org.eclipse.collections.api.list.primitive.LongList
import org.eclipse.collections.api.map.MapIterable
import org.eclipse.collections.impl.factory.Lists
import org.eclipse.collections.impl.utility.LazyIterate
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.cluster.routing.ShardRouting
import org.elasticsearch.index.shard.ShardId


class ShardScoreGroup(
        val shardScoreGroupDescription: ShardScoreGroupDescription,
        val nodeShardGroupScorer: NodeShardGroupScorer) {

    companion object {
        fun buildShardScoreGroupsForIndexes(
                metaData: MetaData,
                shardSizes: MapIterable<ShardId, IndexSizingGroup.ShardSizeInfo>,
                numberOfNodes: Int,
                totalCapacityUnits: Double): ListIterable<ShardScoreGroup> {
            return metaData
                    .let { LazyIterate.adapt(it) }
                    .flatCollect { buildShardScoreGroupsForIndex(it, shardSizes, numberOfNodes, totalCapacityUnits) }
                    .toList()
                    .apply{ this.add(
                            buildGlobalShardScoreGroup(
                                    metaData,
                                    shardSizes,
                                    numberOfNodes,
                                    totalCapacityUnits)) }
        }

        fun buildGlobalShardScoreGroup(
                metaData: MetaData,
                shardSizes: MapIterable<ShardId, IndexSizingGroup.ShardSizeInfo>,
                numberOfNodes: Int,
                totalCapacityUnits: Double): ShardScoreGroup {
            val allIndexes = LazyIterate.adapt(metaData)
            val indexNames = allIndexes.collect { it.index }.toSet()
            val indexSizesGroupedByIndex = shardSizes
                    .keyValuesView()
                    .select { it.one.index in indexNames }
                    .aggregateBy({ it.one.index }, { 0L }, { sum, it -> it.two.estimatedSize + sum })
            val totalShardSizes = allIndexes.sumOfLong { (1 + it.numberOfReplicas) * indexSizesGroupedByIndex[it.index] }
            val scaler = Math.log(numberOfNodes.toDouble()) / Math.log(2.0)

            return ShardScoreGroup(
                    ShardScoreGroupDescription("*", includesPrimaries = true, includesReplicas = true),
                    AveragingShardSizeScorer(totalShardSizes / totalCapacityUnits, scaler = scaler))
        }

        private fun buildShardScoreGroupsForIndex(
                indexMetaData: IndexMetaData,
                shardSizes: MapIterable<ShardId, IndexSizingGroup.ShardSizeInfo>,
                numberOfNodes: Int,
                totalCapacityUnits: Double): RichIterable<ShardScoreGroup> {

            val totalReplicas = indexMetaData.totalNumberOfShards - indexMetaData.numberOfShards
            val indexSizesGroupedByIndex = shardSizes
                    .keyValuesView()
                    .select { it.one.index == indexMetaData.index }
                    .aggregateBy({ it.one.index }, { 0L }, { sum, it -> it.two.estimatedSize + sum })

            val totalPrimarySize = indexSizesGroupedByIndex[indexMetaData.index]
            val totalReplicaSize = indexMetaData.numberOfReplicas * totalPrimarySize
            val totalShardSizes = totalPrimarySize + totalReplicaSize

            if (indexMetaData.totalNumberOfShards <= numberOfNodes) {
                return Lists.immutable.of(ShardScoreGroup(
                        ShardScoreGroupDescription(indexMetaData.index.name, includesPrimaries = true, includesReplicas = true),
                        MinimizeShardCountScorer(totalShardSizes.toDouble() / indexMetaData.totalNumberOfShards)))
            }

            val primaryScoreGroup = ShardScoreGroup(
                    ShardScoreGroupDescription(indexMetaData.index.name, includesPrimaries = true, includesReplicas = false),
                    if (indexMetaData.numberOfShards <= numberOfNodes) MinimizeShardCountScorer(totalPrimarySize.toDouble() / indexMetaData.numberOfShards)
                    else AveragingShardSizeScorer(totalPrimarySize / totalCapacityUnits))

            if (totalReplicas == 0) { return Lists.immutable.of(primaryScoreGroup) }
            
            val replicaScoreGroup = ShardScoreGroup(
                    ShardScoreGroupDescription(indexMetaData.index.name, includesPrimaries = false, includesReplicas = true),
                    if (totalReplicas <= numberOfNodes) MinimizeShardCountScorer(totalReplicaSize.toDouble() / totalReplicas)
                    else AveragingShardSizeScorer(totalReplicaSize / totalCapacityUnits))

            return Lists.immutable.of(primaryScoreGroup, replicaScoreGroup)
        }
    }
}

data class ShardScoreGroupDescription(
        val index: String,
        val includesPrimaries: Boolean,
        val includesReplicas: Boolean) {

    fun isShardIncluded(shard: ShardRouting): Boolean =
            index == "*" ||
            (shard.index == index && (shard.primary() && includesPrimaries || !shard.primary() && includesReplicas))
}

interface NodeShardGroupScorer {
    fun calculateCapacityScore(shardSizes: LongList, nodeCapacity: Double): Double

    fun calculateRelativeScore(shardSizes: LongList, nodeCapacity: Double): Double

    fun calculateBalanceScore(shardSizes: LongList, nodeCapacity: Double): Double =
            calculateRelativeScore(shardSizes, nodeCapacity).let { it * it }
}

class MinimizeShardCountScorer(val averageShardSize: Double) : NodeShardGroupScorer {
    init {
        if (averageShardSize.isNaN()) { throw IllegalArgumentException("found invalid average shard size") }
    }

    override fun calculateCapacityScore(shardSizes: LongList, nodeCapacity: Double): Double {
        if (nodeCapacity == 0.0) { return 1.0 }

        return when (shardSizes.size()) {
            0 -> -1.0
            1 -> 0.0
            else -> calculateRelativeScore(shardSizes, nodeCapacity)
        }
    }

    override fun calculateRelativeScore(shardSizes: LongList, nodeCapacity: Double): Double {
        return when {
            averageShardSize == 0.0 -> 0.0
            nodeCapacity == 0.0 -> shardSizes.sum() / averageShardSize
            else -> when (shardSizes.size()) {
                0, 1 -> 0.0
                else -> shardSizes.toSortedList().let { (it.sum() - it.last) / averageShardSize }
            }
        }
    }
}

class AveragingShardSizeScorer(
        private val averageNodeCapacity: Double,
        private val scaler: Double = 1.0) : NodeShardGroupScorer {

    init {
        if (averageNodeCapacity.isNaN()) { throw IllegalArgumentException("found invalid average node capacity") }
    }

    override fun calculateCapacityScore(shardSizes: LongList, nodeCapacity: Double): Double {
        if (nodeCapacity == 0.0) { return 1.0 }
        return calculateRelativeScore(shardSizes, nodeCapacity)
    }

    override fun calculateRelativeScore(shardSizes: LongList, nodeCapacity: Double): Double {
        return when {
            averageNodeCapacity == 0.0 -> 0.0
            nodeCapacity == 0.0 -> shardSizes.sum() / averageNodeCapacity
            else -> scaler * (shardSizes.sum() / (averageNodeCapacity * nodeCapacity) - 1.0)
        }
    }
}

fun ListIterable<ShardScoreGroup>.findScoreGroupDescriptionsForShard(shard: ShardRouting): ListIterable<ShardScoreGroupDescription> {
    return this.collectIf({ it.shardScoreGroupDescription.isShardIncluded(shard) }, {it.shardScoreGroupDescription})
}