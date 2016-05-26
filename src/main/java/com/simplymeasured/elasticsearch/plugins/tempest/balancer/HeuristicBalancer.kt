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

import com.simplymeasured.elasticsearch.plugins.tempest.BalancerState
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.MockDeciders.sameNodeDecider
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.MockDeciders.shardStateDecider
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.MockDeciders.shardIdAlreadyMoving
import org.eclipse.collections.api.block.function.Function2
import org.eclipse.collections.api.block.function.primitive.IntObjectToIntFunction
import org.eclipse.collections.api.list.ListIterable
import org.eclipse.collections.impl.Counter
import org.eclipse.collections.impl.factory.Lists
import org.eclipse.collections.impl.factory.Sets
import org.eclipse.collections.impl.list.mutable.ListAdapter
import org.eclipse.collections.impl.list.mutable.ListAdapter.adapt
import org.eclipse.collections.impl.utility.LazyIterate
import org.elasticsearch.cluster.ClusterInfo
import org.elasticsearch.cluster.InternalClusterInfoService
import org.elasticsearch.cluster.routing.RoutingNode
import org.elasticsearch.cluster.routing.RoutingNodes
import org.elasticsearch.cluster.routing.ShardRouting
import org.elasticsearch.cluster.routing.ShardRoutingState
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider
import org.elasticsearch.cluster.routing.allocation.decider.Decision
import org.elasticsearch.common.component.AbstractComponent
import org.elasticsearch.common.settings.Settings
import org.joda.time.DateTime
import java.util.*
import kotlin.jvm.internal.iterator

/**
 * Main shard balancer
 *
 * For details on the algorithm and usage see the project's README
 */
class HeuristicBalancer(    settings: Settings,
                        val allocation: RoutingAllocation,
                        val shardSizeCalculator: ShardSizeCalculator,
                        val balancerState: BalancerState,
                        val random: Random) : AbstractComponent(settings) {

    private val routingNodes: RoutingNodes = allocation.routingNodes()
    private val deciders: AllocationDeciders = allocation.deciders()
    private val mockDeciders: ListIterable<MockDecider> = Lists.mutable.of(sameNodeDecider, shardStateDecider, shardIdAlreadyMoving, MockFilterAllocationDecider(settings))
    private val baseModelCluster: ModelCluster = ModelCluster(routingNodes, shardSizeCalculator, mockDeciders, random)
    private val concurrentRebalanceSetting: Int = settings.getAsInt(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE, 4).let { if (it == -1) 4 else it }
    private val searchDepthSetting: Int = settings.getAsInt("tempest.balancer.searchDepth", 8)
    private val searchScaleFactor: Int = settings.getAsInt("tempest.balancer.searchScaleFactor", 1000)
    private val bestNQueueSize: Int = settings.getAsInt("tempest.balancer.searchQueueSize", 10)
    private val minimumShardMovementOverhead: Long = settings.getAsLong("tempest.balancer.minimumShardMovementOverhead", 100000000)
    private val maximumAllowedRiskRate: Double = settings.getAsDouble("tempest.balancer.maximumAllowedRiskRate", 1.10)
    private val forceRebalanceThresholdMinutes: Int = settings.getAsInt("tempest.balancer.forceRebalanceThresholdMinutes", 60)
    private val minimumNodeSizeChangeRate: Double = settings.getAsDouble("tempest.balancer.minimumNodeSizeChangeRate", 0.10)
    private val initalClusterScore: Double = baseModelCluster.calculateBalanceScore()
    private val maximumAllowedRisk: Double = baseModelCluster.calculateRisk() * maximumAllowedRiskRate * maximumAllowedRiskRate
    private val noopMoveChain : MoveChain = MoveChain.Companion.buildOptimalMoveChain(Lists.mutable.of(MoveActionBatch(emptyList<MoveAction>(), 0, 0.0, initalClusterScore)))

    init {
        if (logger.isTraceEnabled) {logger.trace(baseModelCluster.toString())}
    }

    /**
     * rebalance the cluster using a heuristic random sampling approach
     */
    fun rebalance(): Boolean {
        updateBalancerState()
        if (!rebalancePreconditionsCheck()) { return false }

        val bestMoveChain = findBestNextMoveChain(baseModelCluster)
        val nextMoveBatch = bestMoveChain.moveBatches.first()

        if (nextMoveBatch.moves.isEmpty()) {
            logger.debug("balanced complete - score: {} ratio: {}", initalClusterScore, baseModelCluster.calculateBalanceRatio())
            balancerState.lastStableStructuralHash = baseModelCluster.calculateStructuralHash()
            balancerState.lastOptimalBalanceFoundDateTime = DateTime.now()
            return false
        }

        for (move in nextMoveBatch.moves) {
            logger.debug("applying move of {} from {} to {}",
                    move.shard.backingShard.shardId(),
                    move.sourceNode.backingNode.node().hostName,
                    move.destNode.backingNode.node().hostName)

            routingNodes.relocate(move.shard.backingShard, move.destNode.nodeId, move.shard.backingShard.expectedShardSize)
        }

        balancerState.lastBalanceChangeDateTime = DateTime.now()

        return true
    }

    private fun updateBalancerState() {
        balancerState.clusterScore = initalClusterScore
        balancerState.clusterRisk = baseModelCluster.calculateRisk()
        balancerState.clusterBalanceRatio = baseModelCluster.calculateBalanceRatio()
    }

    private fun findBestNextMoveChain(modelCluster: ModelCluster) : MoveChain {
        val goodMoveChains = findGoodMoveChains(modelCluster)

        if (goodMoveChains.isEmpty()) {
            return noopMoveChain
        }

        var bestScore = Double.MAX_VALUE
        var bestRisk = Double.MAX_VALUE
        var bestOverhead = Long.MAX_VALUE

        for (moveChain in goodMoveChains) {
            bestScore = Math.min(bestScore, moveChain.score)
            bestRisk = Math.min(bestRisk, moveChain.risk)
            bestOverhead = Math.min(bestOverhead, moveChain.overhead)
        }

        val orderedChains = goodMoveChains.sortedBy { it.overhead / bestOverhead.toDouble() + it.risk / bestRisk + it.score / bestScore }
        return orderedChains.firstOrNull { isValidBatch(it.moveBatches.first()) } ?: noopMoveChain
    }

    private fun isValidBatch(moveBatch: MoveActionBatch): Boolean {
        return moveBatch.moves.all { isValidMove(it) }
    }

    private fun isValidMove(moveAction: MoveAction): Boolean {
        return deciders.canRebalance(moveAction.shard.backingShard, allocation).type() == Decision.Type.YES &&
               deciders.canAllocate(moveAction.shard.backingShard, moveAction.destNode.backingNode, allocation).type() == Decision.Type.YES
    }

    fun findGoodMoveChains(modelCluster: ModelCluster): List<MoveChain> {
        val searchWindowSize = searchScaleFactor * concurrentRebalanceSetting;
        val bestNQueue = MinimumNQueue<MoveChain>(bestNQueueSize, {it.score})
        val searchCounter = Counter();

        while (searchCounter.count < searchWindowSize) {
            try {
                val hypotheticalCluster = ModelCluster(modelCluster)
                val moveChain = createRandomMoveChain(hypotheticalCluster, concurrentRebalanceSetting, searchDepthSetting)

                if (    moveChain.score < initalClusterScore &&
                        moveChain.risk <= maximumAllowedRisk &&
                        bestNQueue.tryAdd(moveChain)) {
                    searchCounter.reset()
                }
            } catch (e : NoLegalMoveFound) { /* ignore */}
            searchCounter.increment()
        }

        return bestNQueue.asList().filter { calculateBestNodeUsageImprovementFromBase(it) >= minimumNodeSizeChangeRate }
    }

    private fun calculateBestNodeUsageImprovementFromBase(it: MoveChain) = baseModelCluster.findBestNodeUsageImprovement(ModelCluster(baseModelCluster).apply { applyMoveChain(it) })

    private fun createRandomMoveChain(modelCluster: ModelCluster, maxBatchSize: Int, searchDepth: Int): MoveChain {
        val moveBatches : MutableList<MoveActionBatch> = Lists.mutable.empty()

        for (depth in 1..searchDepth) {
            val nextBatchSize = random.nextInt(maxBatchSize) + 1
            val nextMoveBatch = createRandomMoveBatch(modelCluster, nextBatchSize)
            if (nextMoveBatch.risk > maximumAllowedRisk) { break; }
            moveBatches.add(nextMoveBatch)
        }

        return MoveChain.buildOptimalMoveChain(moveBatches)
    }

    private fun createRandomMoveBatch(modelCluster: ModelCluster, size: Int) : MoveActionBatch {
        val moves: MutableList<MoveAction> = Lists.mutable.empty()

        for (moveNumber in 1..size) {
            val move = modelCluster.makeRandomMove(moves)
            moves.add(move)
        }

        val risk = modelCluster.calculateRisk();
        modelCluster.stabilizeCluster()
        val score = modelCluster.calculateBalanceScore();
        val overhead = moves.map { Math.max(it.shard.size, minimumShardMovementOverhead) }.sum()

        return MoveActionBatch(moves, overhead, risk, score)
    }

    private fun rebalancePreconditionsCheck() : Boolean {
        if (routingNodes.count() < 2) {
            logger.debug("not enough nodes to balance")
            return false
        }

        if (routingNodes.shards { it?.started()?:false == false }.count() > 0) {
            logger.debug("found non-started or relocating shards, waiting for cluster to stabilize")
            return false
        }

        if (routingNodes.shardsWithState(ShardRoutingState.STARTED).isEmpty()) {
            logger.debug("could not find any started shards to balance")
            return false
        }

        if (concurrentRebalanceSetting == 0) {
            logger.debug("rebalance disabled")
            return false;
        }

        if (initalClusterScore == 0.0) {
            // this condition can occur during restarts where the cluster services are not
            // fully started yet and thus report "0" size for shards
            logger.debug("cluster score is 0")
            return false
        }

        if (DateTime.now().minusMinutes(forceRebalanceThresholdMinutes).isAfter(balancerState.lastOptimalBalanceFoundDateTime) &&
            DateTime.now().minusMinutes(forceRebalanceThresholdMinutes).isAfter(balancerState.lastBalanceChangeDateTime)) {
            logger.debug("forcing rebalance due to time threshold expiration")
            return true;
        }

        if (baseModelCluster.calculateStructuralHash() == balancerState.lastStableStructuralHash ) {
            logger.debug("cluster appears to already be balanced")
            return false;
        }

        return true
    }

    /**
     * Attempt to allocate unallocated shards using a round-robin scheme
     *
     * Note: This does not leverage random sampling like the rebalance method. Instead, the goal is to get the shards
     *       allocated quickly and then let the rebalancer logic move any small shards around.
     */
    fun allocateUnassigned(): Boolean {
        if (!routingNodes.hasUnassignedShards() || routingNodes.count() <= 1) { return false }

        val unassignedShards = routingNodes.unassigned()
                                           .toList()
                                           .sortedBy { shardSizeCalculator.estimateShardSize(it) }
                                           .reversed()

        val modelCluster = ModelCluster(baseModelCluster)
        var changed = false

        for (unassignedShard in unassignedShards) {
            val bestModelNodes = modelCluster.findBestNodesForShard(unassignedShard)
            val allocatedNode = tryAllocation(unassignedShard, bestModelNodes)

            if (allocatedNode != null) {
                modelCluster.applyShardInitialization(unassignedShard, allocatedNode)
                changed = true
            }
        }

        return changed
    }

    /**
     * Attempt to move shards that can no longer be allocated to a node using a round-robin scheme
     *
     * Note: This does not leverage random sampling like the rebalance method. Instead, the goal is to get the shards
     *       allocated quickly and then let the rebalancer logic move any small shards around.
     */
    fun moveShards(): Boolean {
        val shardsThatMustMove = Lists.mutable.ofAll(routingNodes.shards { shouldMove(it!!) })
                                              .sortThisByLong { shardSizeCalculator.estimateShardSize(it) }
                                              .reverseThis()
        val modelCluster = ModelCluster(baseModelCluster)
        var changed = false

        for (shardThatMustMove in shardsThatMustMove) {
            val bestModelNodes = modelCluster.findBestNodesForShard(shardThatMustMove)
            val allocatedNode = tryMove(shardThatMustMove, bestModelNodes)

            if (allocatedNode != null) {
                modelCluster.applyShardInitialization(shardThatMustMove, allocatedNode)
                changed = true
            }
        }

        return changed
    }

    private fun shouldMove(shard: ShardRouting) = deciders.canRemain(shard, routingNodes.node(shard.currentNodeId()), allocation).type() == Decision.Type.NO

    private fun tryMove(shard: ShardRouting, nodes: ListIterable<ModelNode>) : ModelNode? {
        val shardSize = shardSizeCalculator.estimateShardSize(shard)

        for (node in nodes) {
            if (deciders.canAllocate(shard, node.backingNode, allocation) == Decision.NO) { continue }
            if (deciders.canRebalance(shard, allocation) == Decision.NO) { continue }

            routingNodes.initialize(shard, node.backingNode.nodeId(), shardSize)
            return node
        }

        return null
    }

    private fun tryAllocation(shard: ShardRouting, nodes: ListIterable<ModelNode>) : ModelNode? {
        val shardSize = shardSizeCalculator.estimateShardSize(shard)

        for (node in nodes) {
            val decision = deciders.canAllocate(shard, node.backingNode, allocation)
            if (decision == Decision.NO) { continue }

            routingNodes.initialize(shard, node.backingNode.nodeId(), shardSize)
            return node
        }

        return null
    }
}

