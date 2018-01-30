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

import com.simplymeasured.elasticsearch.plugins.tempest.balancer.*
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.IndexSizingGroup.ShardSizeInfo
import org.eclipse.collections.api.list.ListIterable
import org.eclipse.collections.api.list.MutableList
import org.eclipse.collections.api.map.MapIterable
import org.eclipse.collections.impl.block.factory.Comparators
import org.eclipse.collections.impl.factory.Lists
import org.eclipse.collections.impl.tuple.Tuples.pair
import org.elasticsearch.cluster.routing.ShardRouting
import org.elasticsearch.cluster.routing.ShardRoutingState
import org.elasticsearch.index.shard.ShardId
import java.util.*

/**
 * Represents a possible cluster future easy/cheap function for manipulation
 */
class ModelCluster (
        val modelNodes: ListIterable<ModelNode>,
        private val mockDeciders: ListIterable<MockDecider>,
        private val shardSizes: MapIterable<ShardId, ShardSizeInfo>,
        private val shardScoreGroups: ListIterable<ShardScoreGroup>,
        private val expungeBlacklistedNodes: Boolean,
        private val random: Random) {

    /**
     * deep copy constructor
     */
    constructor(other: ModelCluster) :
        this(modelNodes = other.modelNodes.collect(::ModelNode),
             mockDeciders = other.mockDeciders,
             shardSizes = other.shardSizes,
             shardScoreGroups = other.shardScoreGroups,
             expungeBlacklistedNodes = other.expungeBlacklistedNodes,
             random = other.random)

    companion object {
        val MAX_MOVE_ATTEMPTS = 1000
    }

    private val sourceNodes: ListIterable<ModelNode> =
            if (expungeableShardsExist()) modelNodes.select { it.isExpunging && it.shards.notEmpty() }
            else                          modelNodes.select { !it.isBlacklisted && it.shards.notEmpty()}

    private val destinationNodes: ListIterable<ModelNode> = modelNodes.select { !it.isBlacklisted }

    fun expungeableShardsExist() = modelNodes.select { it.isExpunging }
                                                     .anySatisfy { it.shards.notEmpty() }

    fun makeRandomMoves(numberOfMoves: Int): MutableList<MoveAction> {
        val sourceNodeShardGroupPairs = sourceNodes
                .flatCollect { node ->
                    node.shardManager.shardScoreGroupDetails
                            .keyValuesView()
                            .collectIf({ it.one.index != "*"}, {it.two})
                            .collectIf({ it.shards.notEmpty() }, { group -> pair(node, group) })
                            .toList()}
        val moves = Lists.mutable.empty<MoveAction>()

        for (attempt in 0..MAX_MOVE_ATTEMPTS) {
            val sourceNameShardPair = sourceNodeShardGroupPairs
                    .selectRandomWeightedElement(random, weight = { it.two.relativeScore + 2.0 })
                    ?.let { pair(it.one, it.two.shards.selectRandomElement(random)) }
                    ?: continue

            val sourceNode = sourceNameShardPair.one
            val shard = sourceNameShardPair.two
            val destNode = destinationNodes
                    .collect { pair(it, it.shardManager.shardScoreGroupDetails[shard.scoreGroupDescriptions.first()])}
                    .selectRandomNormalizedWeightedElement(random, weight = { it.two.capacityScore })
                    ?.one
                    ?: continue

            if (!mockDeciders.all { it.canMove(shard, destNode, moves) }) continue

            sourceNode.shardManager.updateState(shard, ShardRoutingState.RELOCATING)
            destNode.shardManager.addShard(shard.copy(state = ShardRoutingState.INITIALIZING))

            MoveAction(
                    sourceNode = sourceNode.backingNode,
                    shard = shard.backingShard,
                    destNode = destNode.backingNode,
                    overhead = shard.shardSizeInfo.actualSize,
                    shardScoreGroupDescriptions = shard.scoreGroupDescriptions)
                    .also { moves.add(it) }
            if (moves.size >= numberOfMoves) { break }
        }

        return moves
    }

    fun canApplyMoveActions(moves: ListIterable<MoveAction>): Boolean {
        for (move in moves) {
            val localSourceNode = sourceNodes.find { it.nodeId == move.sourceNode.nodeId() } ?: return false
            val localDestNode = destinationNodes.find { it.nodeId == move.destNode.nodeId() } ?: return false
            val localShard = localSourceNode.shards.find { it.backingShard.shardId() == move.shard.shardId() } ?: return false
            val otherMoves = moves.reject { it == move }

            if (!mockDeciders.all { it.canMove(localShard, localDestNode, otherMoves) }) {
                return false
            }
        }

        return true
    }

    fun applyMoveChain(moveChain: MoveChain) {
        moveChain.moveBatches.forEach { applyMoveActionBatch(it, stabilize = true) }
    }

    fun applyMoveActionBatch(moveActionBatch: MoveActionBatch, stabilize: Boolean) {
        moveActionBatch.moves.forEach { applyMoveAction(it, stabilize) }
    }

    fun applyMoveActions(moves: ListIterable<MoveAction>, stabilize: Boolean) {
        moves.forEach { applyMoveAction(it, stabilize) }
    }

    fun applyMoveAction(moveAction: MoveAction, stabilize: Boolean) {
        // we have to localize a move action since model nodes and shards are owned by their respective model clusters;
        // this method is not fast O(n) in terms of shard and node searching but is also used in non critical paths
        val localSourceNode = modelNodes.find { it.nodeId == moveAction.sourceNode.nodeId() }!!
        val localDestNode = modelNodes.find { it.nodeId == moveAction.destNode.nodeId() }!!
        val localShard = localSourceNode.shards.find { it.backingShard.isSameShard(moveAction.shard) }!!

        if (stabilize) {
            localSourceNode.shardManager.removeShard(localShard)
            localDestNode.shardManager.addShard(localShard)
        }
        else {
            localSourceNode.shardManager.updateState(localShard, ShardRoutingState.RELOCATING)
            localDestNode.shardManager.addShard(localShard.copy(state = ShardRoutingState.INITIALIZING))
        }
    }

    fun applyShardInitialization(shard: ShardRouting, allocatedNode: ModelNode) {
        val modelShard = ModelShard(
                shard,
                shardSizes[shard.shardId()],
                shardScoreGroups.findScoreGroupDescriptionsForShard(shard))
        modelShard.state = ShardRoutingState.INITIALIZING
        allocatedNode.shardManager.addShard(modelShard)
    }

    /**
     * During a model move the cluster will have some shares in two places: the source in a state of relocating and the dest
     * in a state of initialization. This method will remove shares in the relocating state and "start" initializing shards
     */
    fun stabilizeCluster() {
        modelNodes.forEach { it.stabilizeNode() }
    }

    /**
     * Calculate the balance score for the entire cluster
     *
     * NOTE: for blacklisted nodes while an expunge is happening we treat their capacity as 0 since we are trying to
     *       move everything off of them
     */
    fun calculateBalanceScore(): Double = modelNodes.fold(
            0.0,
            { score, node -> score + node.shardManager.balanceScore })

    /**
     * Calculate the risk for the cluster.
     *
     * Note this is currently defined in terms of the size of the most over-capacity node in the cluster
     */
    fun calculateRisk(): Double = destinationNodes
            .collect { it.calculateNormalizedNodeUsage() }
            .max()
            ?: 0.0

    fun calculateShardScores(): Map<String, Double> =
            modelNodes.toMap({it.backingNode.node().hostName},{it.shardManager.balanceScore })

    private fun safeRateDivide(top: Double, bottom: Double) : Double {
        if (top == 0.0 && bottom == 0.0) { return 1.0 }
        if (bottom == 0.0) { return Double.MAX_VALUE; }
        return top/bottom
    }

    fun findBestNodesForShard(unassignedShard: ShardRouting) : ListIterable<ModelNode> {
        val modelShard = ModelShard(
                unassignedShard,
                shardSizes[unassignedShard.shardId()],
                shardScoreGroups.findScoreGroupDescriptionsForShard(unassignedShard))
        // NOTE: Picking the first score group descriptor as the driver for sorting is arbitrary but is probably good
        // enough in most cases. Technically we should probably do some form of aggregate but this could bias the
        // sorting heavily for every small shards in a large environments
        return destinationNodes
                .select { node -> mockDeciders.all { decider -> decider.canAllocate(modelShard, node) } }
                .toSortedList(Comparators.chain(
                        Comparators.byFunction<ModelNode, Double> { node -> node.shardManager.hypotheticalScoreChange(modelShard)},
                        Comparators.byFunction<ModelNode, Int> { node -> node.shardManager.shardScoreGroupDetails[modelShard.scoreGroupDescriptions.first()].shards.size},
                        Comparators.byFunction<ModelNode, Long> { node -> node.shardManager.totalShardSizes},
                        Comparators.byFunction<ModelNode, Int> { node -> node.shards.size()}))
    }

    override fun toString(): String {
        return modelNodes
                .flatMap { it.shardManager.shards }
                .map { "${it.backingShard.currentNodeId()} ${it.backingShard.index} ${it.backingShard.id} ${it.state} (${it.shardSizeInfo.estimatedSize}/${it.shardSizeInfo.actualSize})" }
                .toList()
                .sorted()
                .joinToString("\n")
    }
}

