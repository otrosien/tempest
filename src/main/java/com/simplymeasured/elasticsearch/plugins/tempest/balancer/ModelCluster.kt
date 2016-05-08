package com.simplymeasured.elasticsearch.plugins.tempest.balancer

import org.eclipse.collections.impl.factory.Sets
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

    constructor(routingNodes: RoutingNodes, clusterInfo: ClusterInfo, mockDeciders: List<MockDecider>, random: Random) :
        this (routingNodes.map { ModelNode(it, clusterInfo) },
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

    fun stabilizeCluster() {
        modelNodes.forEach { it.stabilizeNode() }
    }

    fun calculateBalanceScore(): Double = modelNodes.fold(0.0, { score, node -> score + node.calculateNodeScore(expectedUnitCapacity) })

    fun calculateRisk(): Double = modelNodes.map { it.calculateNodeScore(expectedUnitCapacity) }.max() ?: 0.0

    fun calculateBalanceRatio() : Double = modelNodes
            .map { it.calculateUsage() }
            .filter { it > 0 }
            .let { (it.max() ?: Long.MAX_VALUE).toDouble()  / (it.min() ?: Long.MAX_VALUE).toDouble() }

    private fun calculateExpectedUnitCapacity(): Double {
        val shards = modelNodes.flatMap { it.shards }
        val totalShardSize = shards.map { it.size }.filterNot { it < 0 }.sum().toDouble()
        val totalNodeCapacity = modelNodes.map { it.allocationScale }.sum()
        return totalShardSize / totalNodeCapacity
    }
}

class ModelNode(val backingNode: RoutingNode, val nodeId: String, val shards: MutableList<ModelShard>, val allocationScale: Double) {
    constructor(other: ModelNode) :
        this(other.backingNode, other.nodeId, other.shards.map { it.copy() }.toMutableList(), other.allocationScale)

    constructor(routingNode: RoutingNode, clusterInfo: ClusterInfo) :
        this(routingNode,
             routingNode.nodeId(),
             routingNode.copyShards().map { ModelShard(it, clusterInfo.getShardSize(it, 0)) }.toMutableList(),
             routingNode.node().attributes.getOrElse("allocation.scale", { "1.0" }).toDouble())

    fun calculateNodeScore(expectedUnitCapacity: Double): Double {
        val nodeUsage = shards.map { it.size }.filter { it >= 0 }.sum()
        val scaledUsage = nodeUsage / allocationScale
        return (expectedUnitCapacity - scaledUsage).let { it * it }
    }

    fun calculateUsage() : Long = shards.map { it.size }.sum()

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

data class ModelShard(val index: String, val id: Int, var state: ShardRoutingState, val primary: Boolean, val size: Long, val backingShard: ShardRouting) {
    constructor(shardRouting: ShardRouting, size: Long) : this(shardRouting.index(), shardRouting.id(), shardRouting.state(), shardRouting.primary(), size, shardRouting)
}
