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

class ModelCluster private constructor(val modelNodes: List<ModelNode>, val mockDeciders: List<MockDecider>, val random: Random) {
    companion object {
        val MAX_MOVE_ATTEMPTS = 1000
    }

    val expectedUnitCapacity: Double = calculateExpectedUnitCapacity()

    constructor(routingNodes: RoutingNodes, shardSizeCalculator: ShardSizeCalculator, mockDeciders: List<MockDecider>, random: Random) :
        this (routingNodes.map { ModelNode(it, shardSizeCalculator) },
              mockDeciders,
              random)

    constructor(other: ModelCluster) :
        this(other.modelNodes.map { ModelNode(it) },
             other.mockDeciders,
             other.random)

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
        val localSourceNode = modelNodes.find { it.nodeId == moveAction.sourceNode.nodeId }!!
        val localDestNode = modelNodes.find { it.nodeId == moveAction.destNode.nodeId }!!
        val localShard = localSourceNode.shards.find { it.backingShard.isSameShard(moveAction.shard.backingShard) }!!

        localSourceNode.shards.remove(localShard)
        localDestNode.shards.add(localShard)
    }

    fun stabilizeCluster() {
        modelNodes.forEach { it.stabilizeNode() }
    }

    fun calculateBalanceScore(): Double = modelNodes.fold(0.0, { score, node -> score + node.calculateNodeScore(expectedUnitCapacity) })

    fun calculateRisk(): Double = modelNodes.map { it.calculateNodeScore(expectedUnitCapacity) }.max() ?: 0.0

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

class ModelNode(val backingNode: RoutingNode, val nodeId: String, val shards: MutableList<ModelShard>, val allocationScale: Double) {
    constructor(other: ModelNode) :
        this(other.backingNode, other.nodeId, other.shards.map { it.copy() }.toMutableList(), other.allocationScale)

    constructor(routingNode: RoutingNode, shardSizeCalculator: ShardSizeCalculator) :
        this(routingNode,
             routingNode.nodeId(),
             routingNode.copyShards().map { ModelShard(it, shardSizeCalculator.actualShardSize(it), shardSizeCalculator.estimateShardSize(it)) }.toMutableList(),
             routingNode.node().attributes.getOrElse("allocation.scale", { "1.0" }).toDouble())

    fun calculateNodeScore(expectedUnitCapacity: Double): Double {
        val nodeUsage = shards.map { it.size }.filter { it >= 0 }.sum()
        val scaledUsage = nodeUsage / allocationScale
        return (expectedUnitCapacity - scaledUsage).let { it * it }
    }

    fun calculateUsage() : Long = shards.map { it.estimatedSize }.sum()

    fun calculateNormalizedNodeUsage() : Double = calculateUsage()/allocationScale

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

data class ModelShard(val index: String, val id: Int, var state: ShardRoutingState, val primary: Boolean, val size: Long, val estimatedSize: Long, val backingShard: ShardRouting) {
    constructor(shardRouting: ShardRouting, size: Long, estimatedSize: Long) : this(shardRouting.index(), shardRouting.id(), shardRouting.state(), shardRouting.primary(), size, estimatedSize, shardRouting)
}
