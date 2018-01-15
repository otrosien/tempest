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

import org.eclipse.collections.api.RichIterable
import org.eclipse.collections.api.list.ListIterable
import org.eclipse.collections.api.list.MutableList
import org.eclipse.collections.api.list.primitive.LongList
import org.eclipse.collections.api.map.MapIterable
import org.eclipse.collections.api.set.MutableSet
import org.eclipse.collections.api.set.SetIterable
import org.eclipse.collections.impl.factory.Lists
import org.eclipse.collections.impl.factory.Sets
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import org.elasticsearch.cluster.routing.ShardRoutingState

class NodeShardManager private constructor(
        private val _shards: MutableList<ModelShard>,
        val allocationScale: Double,
        val shardScoreGroupDetails: MapIterable<ShardScoreGroupDescription, ShardScoreGroupDetails>) {

    val shards: ListIterable<ModelShard> get() { return _shards }

    var balanceScore: Double = 0.0
        private set
    var capacityScore: Double = 0.0
        private set
    var relativeScore: Double = 0.0
        private set
    var totalShardSizes: Long = 0L
        private set

    private val _nonStartedNodes: MutableSet<ModelShard> = Sets.mutable.empty()
    val nonStartedNodes: SetIterable<ModelShard>
        get() { return _nonStartedNodes }

    init {
        mapShardsToScoreGroups()
    }

    constructor(
            shards: MutableList<ModelShard>,
            allocationScale: Double,
            shardScoreGroups: RichIterable<ShardScoreGroup>): this(
                _shards = shards,
                allocationScale = allocationScale,
                shardScoreGroupDetails = buildScoreGroupMap(shardScoreGroups)) {
        shards.asLazy().select { it.state != ShardRoutingState.STARTED }.into(_nonStartedNodes)
        updateScores()
    }

    constructor(other: NodeShardManager): this(
            _shards = other._shards.collect { it.copy() },
            allocationScale = other.allocationScale,
            shardScoreGroupDetails = buildScoreGroupMapLike(other.shardScoreGroupDetails)) {
        balanceScore = other.balanceScore
        capacityScore = other.capacityScore
        relativeScore = other.relativeScore
        totalShardSizes = other.totalShardSizes
        _nonStartedNodes.addAllIterable(other._nonStartedNodes)
    }

    companion object {
        private fun buildScoreGroupMap(shardScoreGroups: RichIterable<ShardScoreGroup>): MapIterable<ShardScoreGroupDescription, ShardScoreGroupDetails> {
            return UnifiedMap.newMap<ShardScoreGroupDescription, ShardScoreGroupDetails>(shardScoreGroups.size() + 1)
                    .apply { shardScoreGroups.forEach {
                        this[it.shardScoreGroupDescription] = ShardScoreGroupDetails(it.nodeShardGroupScorer)
                    }}
        }

        private fun buildScoreGroupMapLike(other: MapIterable<ShardScoreGroupDescription, ShardScoreGroupDetails>): MapIterable<ShardScoreGroupDescription, ShardScoreGroupDetails> {
            return UnifiedMap.newMap<ShardScoreGroupDescription, ShardScoreGroupDetails>(other.size())
                    .apply {
                        other.forEachKeyValue { desc, details ->
                            this[desc] = ShardScoreGroupDetails(details.nodeShardGroupScorer).apply {
                                this.capacityScore = details.capacityScore
                                this.balanceScore = details.balanceScore
                                this.relativeScore = details.relativeScore
                            }
                        }
                    }
        }
    }

    private fun mapShardsToScoreGroups() {
        _shards.forEach { shard ->
            shard.scoreGroupDescriptions.forEach { desc ->
                shardScoreGroupDetails[desc].shards.add(shard)
            }
        }
    }

    private fun updateScores() {
        totalShardSizes = _shards.sumOfLong { it.shardSizeInfo.estimatedSize }
        shardScoreGroupDetails.valuesView().forEach { updateScoresForGroup(it) }
    }

    fun addShard(shard: ModelShard) {
        _shards.add(shard)
        if (shard.state != ShardRoutingState.STARTED) { _nonStartedNodes.add(shard) }
        totalShardSizes += shard.shardSizeInfo.estimatedSize
        shard.scoreGroupDescriptions
             .collect { shardScoreGroupDetails[it] }
             .tap {it.shards.add(shard)}
             .forEach { updateScoresForGroup(it) }

    }

    fun removeShard(shard: ModelShard) {
        _shards.remove(shard)
        if (shard.state != ShardRoutingState.STARTED) { _nonStartedNodes.remove(shard) }
        totalShardSizes -= shard.shardSizeInfo.estimatedSize
        shard.scoreGroupDescriptions
                .collect { shardScoreGroupDetails[it] }
                .tap { it.shards.remove(shard) }
                .forEach { updateScoresForGroup(it) }
    }

    private fun updateScoresForGroup(details: ShardScoreGroupDetails) {
        val oldCapacityScore = details.capacityScore
        val oldBalanceScore = details.balanceScore
        val oldRelativeScore = details.relativeScore

        details.capacityScore = details.nodeShardGroupScorer.calculateCapacityScore(details.shardSizes, allocationScale)
        details.balanceScore = details.nodeShardGroupScorer.calculateBalanceScore(details.shardSizes, allocationScale)
        details.relativeScore = details.nodeShardGroupScorer.calculateRelativeScore(details.shardSizes, allocationScale)

        capacityScore += details.capacityScore - oldCapacityScore
        balanceScore += details.balanceScore - oldBalanceScore
        relativeScore += details.relativeScore - oldRelativeScore

        if (capacityScore.isNaN()) throw RuntimeException("Found invalid capacity score")
        if (relativeScore.isNaN()) throw RuntimeException("Found invalid relative score")
        if (balanceScore.isNaN()) throw RuntimeException("Found invalid balance score")
    }

    fun updateState(shard: ModelShard, state: ShardRoutingState) {
        shard.state = state

        when (state) {
            ShardRoutingState.STARTED -> _nonStartedNodes.remove(shard)
            else -> _nonStartedNodes.add(shard)
        }

    }

    fun hypotheticalScoreChange(shard: ModelShard): Double {
        val currentScore = shard.scoreGroupDescriptions
                .asLazy()
                .collectDouble { shardScoreGroupDetails[it].balanceScore }
                .sum()

        val newScore = shard.scoreGroupDescriptions
                .asLazy()
                .collect { shardScoreGroupDetails[it] }
                .collectDouble { it.nodeShardGroupScorer.calculateBalanceScore(LongArrayList.newList(it.shardSizes).apply { add(shard.shardSizeInfo.estimatedSize) }, allocationScale ) }
                .sum()

        return newScore - currentScore
    }
}

class ShardScoreGroupDetails(
        val nodeShardGroupScorer: NodeShardGroupScorer,
        val shards: MutableList<ModelShard> = Lists.mutable.empty()) {

    var balanceScore: Double = 0.0
    var capacityScore: Double = 0.0
    var relativeScore: Double = 0.0

    val shardSizes: LongList
        get() { return shards.collectLong {it.shardSizeInfo.estimatedSize} }
}