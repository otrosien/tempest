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

import org.eclipse.collections.api.list.ListIterable
import org.eclipse.collections.api.list.MutableList
import org.eclipse.collections.api.set.SetIterable
import org.eclipse.collections.api.tuple.Pair
import org.eclipse.collections.impl.block.factory.Comparators
import org.eclipse.collections.impl.block.factory.Functions
import org.eclipse.collections.impl.factory.Sets
import org.eclipse.collections.impl.list.mutable.FastList
import org.eclipse.collections.impl.tuple.Tuples
import org.eclipse.collections.impl.utility.LazyIterate
import org.elasticsearch.cluster.ClusterInfo
import org.elasticsearch.cluster.routing.RoutingNode
import org.elasticsearch.cluster.routing.RoutingNodes
import org.elasticsearch.cluster.routing.ShardRouting
import org.elasticsearch.cluster.routing.ShardRoutingState
import java.util.*

/**
 * Represents a possible cluster future easy/cheap function for manipulation
 */
class ModelCluster private constructor(
        val modelNodes: ListIterable<ModelNode>,
        val mockDeciders: ListIterable<MockDecider>,
        val shardSizeCalculator: ShardSizeCalculator,
        val expungeBlacklistedNodes: Boolean,
        val random: Random) {

    /**
     * main constructor used for creating a base model from ES provided data
     */
    constructor(routingNodes: RoutingNodes,
                blacklistFilter: (RoutingNode) -> Boolean,
                shardSizeCalculator: ShardSizeCalculator,
                mockDeciders: ListIterable<MockDecider>,
                expungeBlacklistedNodes: Boolean,
                random: Random) :
        this (modelNodes = LazyIterate.adapt(routingNodes)
                                      .collect { it -> ModelNode.buildModelNode(
                                              node = it,
                                              expungeBlacklistedNodes = expungeBlacklistedNodes,
                                              blacklistFilter = blacklistFilter,
                                              shardSizeCalculator = shardSizeCalculator) }
                                      .toList(),
              mockDeciders = mockDeciders,
              shardSizeCalculator = shardSizeCalculator,
              expungeBlacklistedNodes = expungeBlacklistedNodes,
              random = random)

    /**
     * deep copy constructor
     */
    constructor(other: ModelCluster) :
        this(modelNodes = other.modelNodes.collect(::ModelNode),
             mockDeciders = other.mockDeciders,
             shardSizeCalculator = other.shardSizeCalculator,
             expungeBlacklistedNodes = other.expungeBlacklistedNodes,
             random = other.random)

    companion object {
        val MAX_MOVE_ATTEMPTS = 1000
    }

    val sourceNodes: ListIterable<ModelNode> = if (expungeableShardsExist()) modelNodes.select { it.isExpunging && it.shards.notEmpty() }
                                               else                          modelNodes.select { !it.isBlacklisted && it.shards.notEmpty()}

    val destinationNodes: ListIterable<ModelNode> = modelNodes.select { !it.isBlacklisted }
    val expectedUnitCapacity: Double = calculateExpectedUnitCapacity()

    private fun expungeableShardsExist() = modelNodes.select { it.isExpunging }
                                                     .anySatisfy { it.shards.isNotEmpty() }

    /**
     * Attempt to make a random move that attempts to satisfy all mock deciders
     */
    fun makeRandomMove(previousMoves: Collection<MoveAction>): MoveAction? {
        for (attempt in 0..MAX_MOVE_ATTEMPTS) {
            val sourceNode = findValidSourceNode()
            if (sourceNode.shards.isEmpty()) { continue }

            val destNode = findValidDestinationNode()
            val shard = sourceNode.shards.get(random.nextInt(sourceNode.shards.size))

            if (mockDeciders.all { it.canMove(shard, destNode, previousMoves) }) {
                shard.state = ShardRoutingState.RELOCATING
                destNode.shards.add(shard.copy(state = ShardRoutingState.INITIALIZING))
                return MoveAction(sourceNode, shard, destNode)
            }
        }

        return null
    }

    private fun findValidDestinationNode() = destinationNodes.get(random.nextInt(destinationNodes.size()))

    private fun findValidSourceNode() = sourceNodes.get(random.nextInt(sourceNodes.size()))

    fun applyMoveChain(moveChain: MoveChain) {
        moveChain.moveBatches.forEach { applyMoveActionBatch(it) }
    }

    fun applyMoveActionBatch(moveActionBatch: MoveActionBatch) {
        moveActionBatch.moves.forEach { applyMoveAction(it) }
    }

    fun applyMoveAction(moveAction: MoveAction) {
        // we have to localize a move action since model nodes and shards are owned by their respective model clusters;
        // this method is not fast O(n) in terms of shard and node searching but is also used in non critical paths
        val localSourceNode = modelNodes.find { it.nodeId == moveAction.sourceNode.nodeId }!!
        val localDestNode = modelNodes.find { it.nodeId == moveAction.destNode.nodeId }!!
        val localShard = localSourceNode.shards.find { it.backingShard.isSameShard(moveAction.shard.backingShard) }!!

        localSourceNode.shards.remove(localShard)
        localDestNode.shards.add(localShard)
    }

    fun applyShardInitialization(shard: ShardRouting, allocatedNode: ModelNode) {
        val modelShard = ModelShard(shard, size = shardSizeCalculator.actualShardSize(shard), estimatedSize = shardSizeCalculator.estimateShardSize(shard))
        modelShard.state = ShardRoutingState.INITIALIZING
        allocatedNode.shards.add(modelShard)
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
            { score, node -> score + node.calculateNodeScore(expectedUnitCapacity) })

    /**
     * Calculate the risk for the cluster.
     *
     * Note this is currently defined in terms of the size of the most over-capacity node in the cluster
     */
    fun calculateRisk(): Double = modelNodes
            .map { it.calculateNodeScore(expectedUnitCapacity) }
            .max() ?: 0.0

    private fun calculateExpectedUnitCapacity(): Double {
        val shards = modelNodes.flatMap { it.shards }
        val totalNodeSize = shards.map { it.estimatedSize }.filterNot { it < 0 }.sum().toDouble()
        val totalClusterCapacity = destinationNodes.map { it.allocationScale }.sum()
        return totalNodeSize / totalClusterCapacity
    }

    /**
     * Find the percent change of the node with the most usage change between this cluster and another cluster
     */
    fun findBestNodeUsageImprovement(otherCluster: ModelCluster): Double = LazyIterate
            .zip(modelNodes, otherCluster.modelNodes)
            .collect { Tuples.pair(it.one.calculateNormalizedNodeUsage(), it.two.calculateNormalizedNodeUsage()) }
            .collect { Math.abs(1.0 - safeRateDivide(it.two, it.one)) }
            .max()

    private fun safeRateDivide(top: Double, bottom: Double) : Double {
        if (top == 0.0 && bottom == 0.0) { return 1.0 }
        if (bottom == 0.0) { return Double.MAX_VALUE; }
        return top/bottom
    }

    fun findBestNodesForShard(unassignedShard: ShardRouting) : ListIterable<ModelNode> {
        val modelShard = ModelShard(unassignedShard, size = shardSizeCalculator.actualShardSize(unassignedShard), estimatedSize = shardSizeCalculator.estimateShardSize(unassignedShard))
        return destinationNodes.select { node -> mockDeciders.all { decider -> decider.canAllocate(modelShard, node) } }
                               .toSortedList(Comparators.chain(
                                       Comparators.byFunction<ModelNode, Double> {it.calculateNormalizedNodeUsage()},
                                       Comparators.byFunction<ModelNode, Int> {it.shards.size}))
    }

    override fun toString(): String {
        return modelNodes
                .flatMap { it.shards }
                .map { "${it.backingShard.currentNodeId()} ${it.backingShard.index} ${it.backingShard.id} ${it.state} (${it.estimatedSize}/${it.size})" }
                .toList()
                .sorted()
                .joinToString("\n")
    }
}

/**
 * Model representation of a Node and its shards
 */
class ModelNode private constructor(
        val backingNode: RoutingNode,
        val nodeId: String,
        val shards: MutableList<ModelShard>,
        val allocationScale: Double,
        val isBlacklisted: Boolean,
        val isExpunging: Boolean) {

    /**
     * Deep Copy Constructor
     */
    constructor(other: ModelNode) :
        this(other.backingNode,
             other.nodeId,
             other.shards.collect { it.copy() },
             other.allocationScale,
             other.isBlacklisted,
             other.isExpunging)

    /**
     * main constructor used when creating a base model from ES data structures
     */
    constructor(routingNode: RoutingNode,
                shardSizeCalculator: ShardSizeCalculator,
                isBlacklisted: Boolean,
                isExpunging: Boolean) :
        this(routingNode,
             routingNode.nodeId(),
             routingNode.copyShards().map { ModelShard(it, shardSizeCalculator.actualShardSize(it), shardSizeCalculator.estimateShardSize(it)) }.toFastList(),
             routingNode.node().attributes.getOrElse("allocation.scale", { "1.0" }).toDouble(),
             isBlacklisted,
             isExpunging)

    companion object {
        internal fun buildModelNode(
                node: RoutingNode,
                shardSizeCalculator: ShardSizeCalculator,
                blacklistFilter: (RoutingNode) -> Boolean,
                expungeBlacklistedNodes: Boolean): ModelNode {

            val blacklisted = blacklistFilter(node)
            return ModelNode(node, shardSizeCalculator, blacklisted, expungeBlacklistedNodes && blacklisted)
        }
    }

    /**
     * Calculate the node's wighted score (used for cluster risk and scoring)
     *
     * Note: currently using R^2 deviation from the norm as a score
     */
    fun calculateNodeScore(expectedUnitCapacity: Double): Double {
        val effectiveCapacity = if (isExpunging) 0.0 else expectedUnitCapacity
        return (effectiveCapacity - calculateNormalizedNodeUsage()).let { it * it }
    }

    /**
     * sum of all shard sizes
     */
    fun calculateUsage() : Long = shards.map { it.estimatedSize }.sum()

    /**
     * Like calculate usage but weighted in terms of the allocation scale used for this node
     */
    fun calculateNormalizedNodeUsage() : Double = calculateUsage()/allocationScale

    /**
     * Remove any relocating shards and set initializing shards to be started
     */
    fun stabilizeNode() {
        val shardIterator = shards.iterator()
        while (shardIterator.hasNext()) {
            val shard = shardIterator.next()
            when (shard.state) {
                ShardRoutingState.RELOCATING -> shardIterator.remove()
                ShardRoutingState.INITIALIZING -> shard.state = ShardRoutingState.STARTED
                else -> {}
            }
        }
    }
}

private fun <T> Iterable<T>.toFastList(): MutableList<T> = FastList.newList(this)

/**
 * Model representation of a shard
 */
data class ModelShard(val index: String, val id: Int, var state: ShardRoutingState, val primary: Boolean, val size: Long, val estimatedSize: Long, val backingShard: ShardRouting) {
    constructor(shardRouting: ShardRouting, size: Long, estimatedSize: Long) : this(shardRouting.index(), shardRouting.id(), shardRouting.state(), shardRouting.primary(), size, estimatedSize, shardRouting)
}
