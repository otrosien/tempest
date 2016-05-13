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

import org.eclipse.collections.api.tuple.Pair
import org.eclipse.collections.impl.factory.Sets
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
class ModelCluster private constructor(val modelNodes: List<ModelNode>, val mockDeciders: List<MockDecider>, val random: Random) {
    companion object {
        val MAX_MOVE_ATTEMPTS = 1000
    }

    val expectedUnitCapacity: Double = calculateExpectedUnitCapacity()

    /**
     * main constructor used for creating a base model from ES provided data
     */
    constructor(routingNodes: RoutingNodes, shardSizeCalculator: ShardSizeCalculator, mockDeciders: List<MockDecider>, random: Random) :
        this (routingNodes.map { ModelNode(it, shardSizeCalculator) },
              mockDeciders,
              random)

    /**
     * deep copy constructor
     */
    constructor(other: ModelCluster) :
        this(other.modelNodes.map { ModelNode(it) },
             other.mockDeciders,
             other.random)

    /**
     * Attempt to make a random move that attempts to satisfy all mock deciders
     *
     * @throws NoLegalMoveFound if no moves can be found after MAX_MOVE_ATTEMPTS
     */
    fun makeRandomMove(previousMoves: Collection<MoveAction>): MoveAction {
        for (attempt in 0..MAX_MOVE_ATTEMPTS) {
            val sourceNode = modelNodes.get(random.nextInt(modelNodes.size))
            if (sourceNode.shards.isEmpty()) { continue }

            val destNode = modelNodes.get(random.nextInt(modelNodes.size))
            val shard = sourceNode.shards.get(random.nextInt(sourceNode.shards.size))

            if (mockDeciders.all { it.canMove(shard, destNode, previousMoves) }) {
                shard.state = ShardRoutingState.RELOCATING
                destNode.shards.add(shard.copy(state = ShardRoutingState.INITIALIZING))
                return MoveAction(sourceNode, shard, destNode)
            }
        }

        throw NoLegalMoveFound()
    }

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

    /**
     * During a model move the cluster will have some shares in two places: the source in a state of relocating and the dest
     * in a state of initialization. This method will remove shares in the relocating state and "start" initializing shards
     */
    fun stabilizeCluster() {
        modelNodes.forEach { it.stabilizeNode() }
    }

    /**
     * Calculate the balance score for the entire cluster
     */
    fun calculateBalanceScore(): Double = modelNodes.fold(0.0, { score, node -> score + node.calculateNodeScore(expectedUnitCapacity) })

    /**
     * Calculate the risk for the cluster.
     *
     * Note this is currently defined in terms of the size of the most over-capacity node in the cluster
     */
    fun calculateRisk(): Double = modelNodes.map { it.calculateNodeScore(expectedUnitCapacity) }.max() ?: 0.0

    /**
     * Calculate the ratio of the most over and under capacity nodes (excluding empty nodes)
     *
     * Note: A ratio of 1.0 is ideal and lowest possible ratio. Good ratios should be between 1.0 and 1.4
     */
    fun calculateBalanceRatio() : Double = modelNodes
            .map { it.calculateNormalizedNodeUsage() }
            .filter { it > 0.0 }
            .let { (it.max() ?: Double.MAX_VALUE)  / (it.min() ?: Double.MAX_VALUE) }

    private fun calculateExpectedUnitCapacity(): Double {
        val shards = modelNodes.flatMap { it.shards }
        val totalNodeSize = shards.map { it.estimatedSize }.filterNot { it < 0 }.sum().toDouble()
        val totalClusterCapacity = modelNodes.map { it.allocationScale }.sum()
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

    /**
     * Calculate a numeric hash that represents the structure of the cluster.
     *
     * Logically, this is attempting to create a hash from an order agnostic representation of the nodes and their shards.
     * Functionally, this is used for determining if the cluster has changed in any meaningful way without persisting
     * the entire model cluster to memory (which would hold onto some critical and possibly large ES structures)
     */
    fun calculateStructuralHash(): Int =  modelNodes.map { it.calculateStructuralHash() }.toHashSet().hashCode()

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
class ModelNode private constructor(val backingNode: RoutingNode, val nodeId: String, val shards: MutableList<ModelShard>, val allocationScale: Double) {
    /**
     * Deep Copy Constructor
     */
    constructor(other: ModelNode) :
        this(other.backingNode, other.nodeId, other.shards.map { it.copy() }.toMutableList(), other.allocationScale)

    /**
     * main constructor used when creating a base model from ES data structures
     */
    constructor(routingNode: RoutingNode, shardSizeCalculator: ShardSizeCalculator) :
        this(routingNode,
             routingNode.nodeId(),
             routingNode.copyShards().map { ModelShard(it, shardSizeCalculator.actualShardSize(it), shardSizeCalculator.estimateShardSize(it)) }.toMutableList(),
             routingNode.node().attributes.getOrElse("allocation.scale", { "1.0" }).toDouble())

    /**
     * Calculate the node's wighted score (used for cluster risk and scoring)
     *
     * Note: currently using R^2 deviation from the norm as a score
     */
    fun calculateNodeScore(expectedUnitCapacity: Double): Double {
        val nodeUsage = shards.map { it.size }.filter { it >= 0 }.sum()
        val scaledUsage = nodeUsage / allocationScale
        return (expectedUnitCapacity - scaledUsage).let { it * it }
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

    fun calculateStructuralHash(): Int = shards.map { (it.index + it.id).hashCode() }.toHashSet().apply { add(nodeId.hashCode()) }.hashCode()
}

/**
 * Model representation of a shard
 */
data class ModelShard(val index: String, val id: Int, var state: ShardRoutingState, val primary: Boolean, val size: Long, val estimatedSize: Long, val backingShard: ShardRouting) {
    constructor(shardRouting: ShardRouting, size: Long, estimatedSize: Long) : this(shardRouting.index(), shardRouting.id(), shardRouting.state(), shardRouting.primary(), size, estimatedSize, shardRouting)
}
