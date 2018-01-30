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

import com.simplymeasured.elasticsearch.plugins.tempest.balancer.model.*
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.model.MockDeciders.sameNodeDecider
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.model.MockDeciders.shardIdAlreadyMoving
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.model.MockDeciders.shardStateDecider
import org.eclipse.collections.api.LazyIterable
import org.eclipse.collections.api.list.ListIterable
import org.eclipse.collections.api.list.MutableList
import org.eclipse.collections.api.map.MapIterable
import org.eclipse.collections.impl.Counter
import org.eclipse.collections.impl.factory.Lists
import org.eclipse.collections.impl.utility.LazyIterate
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.cluster.routing.RoutingNodes
import org.elasticsearch.cluster.routing.ShardRouting
import org.elasticsearch.cluster.routing.ShardRoutingState
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders
import org.elasticsearch.cluster.routing.allocation.decider.Decision
import org.elasticsearch.common.component.AbstractComponent
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.shard.ShardId
import java.lang.Math.abs
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * Main shard balancer
 *
 * For details on the algorithm and usage see the project's README
 */
class HeuristicBalancer(settings: Settings,
                        shardSizeCalculator: ShardSizeCalculator,
                        val allocation: RoutingAllocation,
                        val balancerConfiguration: BalancerConfiguration,
                        val preApprovedMoveDescriptionBatches: ListIterable<ListIterable<MoveDescription>> = Lists.immutable.empty(),
                        val random: Random) : AbstractComponent(settings) {

    private val routingNodes: RoutingNodes = allocation.routingNodes()
    private val deciders: AllocationDeciders = allocation.deciders()
    private val shardSizes: MapIterable<ShardId, IndexSizingGroup.ShardSizeInfo> =
            shardSizeCalculator.buildShardSizeInfo(allocation)
    val baseModelCluster = buildModelCluster(
            routingNodes = allocation.routingNodes(),
            metaData = allocation.metaData(),
            shardSizes = shardSizes,
            settings = settings,
            balancerConfiguration = balancerConfiguration,
            random = random)
    private val initalClusterScore: Double = baseModelCluster.calculateBalanceScore()
    private val expungingMode: Boolean = baseModelCluster.expungeableShardsExist()
    private val maximumAllowedRisk: Double = if (expungingMode) Double.MAX_VALUE
                                             else baseModelCluster.calculateRisk() * balancerConfiguration.maximumAllowedRiskRate
    private val noopMoveChain : MoveChain = MoveChain.noopMoveChain(initalClusterScore)

    var futurePreApprovedMoveDescriptionBatches: ListIterable<ListIterable<MoveDescription>> = Lists.immutable.empty()

    init {
        if (logger.isTraceEnabled) {logger.trace(baseModelCluster.toString())}
    }

    companion object {
        fun buildModelCluster(
                routingNodes: RoutingNodes,
                metaData: MetaData,
                shardSizes: MapIterable<ShardId, IndexSizingGroup.ShardSizeInfo>,
                settings: Settings,
                balancerConfiguration: BalancerConfiguration,
                random: Random): ModelCluster {

            val expungeBlacklistedNodes = balancerConfiguration.expungeBlacklistedNodes
            val blacklistFilter = balancerConfiguration.clusterExcludeFilter

            val mockDeciders: ListIterable<MockDecider> =
                    Lists.mutable.of(
                            sameNodeDecider,
                            shardStateDecider,
                            shardIdAlreadyMoving,
                            MockFilterAllocationDecider(settings))

            val totalCapacityUnits = routingNodes
                    .let { LazyIterate.adapt(it) }
                    .reject { expungeBlacklistedNodes && blacklistFilter.invoke(it) }
                    .sumOfDouble{ it.node().attributes.getOrElse("allocation.scale", { "1.0" }).toDouble() }

            val shardScoreGroups = ShardScoreGroup.buildShardScoreGroupsForIndexes(
                    metaData,
                    shardSizes,
                    routingNodes.count(),
                    totalCapacityUnits)

            val modelNodes = LazyIterate.
                    adapt(routingNodes)
                    .collect { it ->
                        val isBlacklisted = blacklistFilter.invoke(it)
                        ModelNode(
                                routingNode = it,
                                shardSizes = shardSizes,
                                shardScoreGroups = shardScoreGroups,
                                isBlacklisted = isBlacklisted,
                                isExpunging = isBlacklisted && expungeBlacklistedNodes)
                    }
                    .toList()

            return ModelCluster(
                    modelNodes = modelNodes,
                    mockDeciders = mockDeciders,
                    shardSizes = shardSizes,
                    shardScoreGroups = shardScoreGroups,
                    expungeBlacklistedNodes = expungeBlacklistedNodes,
                    random = random)
        }
    }

    /**
     * rebalance the cluster using a heuristic random sampling approach
     */
    fun rebalance(): BalanceDecision {
        if (!rebalancePreconditionsCheck()) return BalanceDecision.ON_HOLD

        val bestMoveChain = findBestNextMoveChain(baseModelCluster)
        val nextMoveBatch = bestMoveChain.moveBatches.firstOrNull()

        if (nextMoveBatch == null) {
            logScoreInfo()
            return BalanceDecision.BALANCED
        }

        nextMoveBatch.moves.forEach { move ->
            logger.debug("applying move of {} from {} to {}",
                    move.shard.shardId(),
                    move.sourceNode.node().hostName,
                    move.destNode.node().hostName)

            routingNodes.relocate(
                    move.shard,
                    move.destNode.nodeId(),
                    move.overhead)
        }

        futurePreApprovedMoveDescriptionBatches = bestMoveChain.moveBatches
                .drop(1)
                .collect { it.buildMoveDescriptions() }

        return BalanceDecision.BALANCING
    }

    private fun logScoreInfo() {
        logger.debug("balanced complete - score: {}", initalClusterScore)
        logger.debug("node scores - {}", baseModelCluster.calculateShardScores())
        baseModelCluster.modelNodes.forEach { node ->
            node.shardManager.shardScoreGroupDetails.forEachKeyValue { groupDescription, groupDetails ->
                logger.trace("group score for node {}:{} - B:{} R:{} C:{}",
                        node.backingNode.node().hostName,
                        groupDescription,
                        groupDetails.balanceScore,
                        groupDetails.relativeScore,
                        groupDetails.capacityScore)
            }
        }
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
        return deciders.canRebalance(moveAction.shard, allocation).type() == Decision.Type.YES &&
               deciders.canAllocate(moveAction.shard, moveAction.destNode, allocation).type() == Decision.Type.YES
    }

    private fun findGoodMoveChains(modelCluster: ModelCluster): ListIterable<MoveChain> {
        val searchWindowSize = balancerConfiguration.searchScaleFactor * balancerConfiguration.concurrentRebalanceSetting
        val bestNQueue = MinimumNQueue<MoveChain>(balancerConfiguration.bestNQueueSize, {it.score})
        val searchCounter = Counter()
        val searchStopTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(balancerConfiguration.searchTimeLimitSeconds)
        
        while (searchCounter.count < searchWindowSize) {
            val hypotheticalCluster = ModelCluster(modelCluster)
            val moveChain = createRandomMoveChain(
                    hypotheticalCluster,
                    balancerConfiguration.concurrentRebalanceSetting,
                    balancerConfiguration.searchDepthSetting)

            if (isGoodChain(moveChain) && bestNQueue.tryAdd(moveChain)) {
                if (System.nanoTime() >= searchStopTime) { break }
                searchCounter.reset()
            }

            searchCounter.increment()
        }

        return bestNQueue.asList()
                .let {
                    if (expungingMode) it.toList()
                    else it.collect { optimizeMoveChain(it, modelCluster) }
                            .select { isGoodChain(it) && hasMinimumNodeUsageImprovementFromBase(it) }
                            .toList() }
                .apply { buildMoveChainFromPreApprovedBatches(modelCluster)?.also { this.add(it) } }
    }

    private fun buildMoveChainFromPreApprovedBatches(modelCluster: ModelCluster): MoveChain? {
        if (preApprovedMoveDescriptionBatches.isEmpty) { return null }

        val hypotheticalCluster = ModelCluster(modelCluster)
        val moveBatches = Lists.mutable.empty<MoveActionBatch>()

        preApprovedMoveDescriptionBatches.forEach { moveDescriptions ->
            val moves = moveDescriptions
                    .collect { it.buildMoveAction(hypotheticalCluster) }
                    .also { if (it.any { it == null }) return null }
                    as ListIterable<MoveAction>

            if (!hypotheticalCluster.canApplyMoveActions(moves)) { return null }

            hypotheticalCluster.applyMoveActions(moves, stabilize = false)
            buildMoveActionBatch(moves, hypotheticalCluster).run { moveBatches.add(this) }
        }

        return MoveChain.fromMoveBatches(moveBatches)
    }


    private fun optimizeMoveChain(moveChain: MoveChain, modelCluster: ModelCluster): MoveChain {
        return moveChain.moveBatches
                .asLazy()
                .flatCollect { it.moves }
                .collect { newChainWithout(moveChain, it.shard.shardId(), modelCluster) }
                .select { it.score < moveChain.score && it.risk <= moveChain.risk }
                .firstOrNull()
                ?.let { optimizeMoveChain(it, modelCluster) }
                ?: moveChain
    }

    private fun newChainWithout(moveChain: MoveChain, shardId: ShardId, modelCluster: ModelCluster): MoveChain {
        val hypotheticalCluster = ModelCluster(modelCluster)
        return moveChain.moveBatches
                .collect { it.moves.reject { it.shard.shardId() == shardId } }
                .collect { moves -> buildMoveActionBatch(moves, hypotheticalCluster.apply { applyMoveActions(moves, stabilize = false) }) }
                .let { MoveChain.fromMoveBatches(it) }
    }

    private fun isGoodChain(moveChain: MoveChain): Boolean =
            expungingMode ||
            moveChain.score < initalClusterScore &&
            moveChain.risk <= maximumAllowedRisk

    private fun hasMinimumNodeUsageImprovementFromBase(moveChain: MoveChain): Boolean {
        val updatedCluster = ModelCluster(baseModelCluster).apply { applyMoveChain(moveChain) }
        val moves = moveChain.moveBatches.flatCollect { it.moves }
        return LazyIterate
                .zip(buildLazyShardGroupChangeIterator(moves, baseModelCluster),
                     buildLazyShardGroupChangeIterator(moves, updatedCluster))
                .collectDouble { it.one - it.two }
                .anySatisfy { it >= balancerConfiguration.minimumNodeSizeChangeRate }
    }

    private fun buildLazyShardGroupChangeIterator(moves: ListIterable<MoveAction>, cluster: ModelCluster): LazyIterable<Double> {
        return moves
                .asLazy()
                .flatCollect { move ->
                    cluster.modelNodes
                            .select { node ->
                                node.backingNode.let { it == move.sourceNode || it == move.destNode } }
                            .collect { node ->
                                node.shardManager.shardScoreGroupDetails[move.shardScoreGroupDescriptions.first()] }
                            .collect { abs(it.relativeScore) } }
    }

    private fun createRandomMoveChain(hypotheticalCluster: ModelCluster, maxBatchSize: Int, searchDepth: Int): MoveChain {
        val moveBatches : MutableList<MoveActionBatch> = Lists.mutable.empty()

        for (depth in 1..searchDepth) {
            val nextBatchSize = random.nextInt(maxBatchSize) + 1
            val nextMoveBatch = createRandomMoveBatch(hypotheticalCluster, nextBatchSize)
            if (nextMoveBatch.risk > maximumAllowedRisk) { break }
            moveBatches.add(nextMoveBatch)
        }

        return MoveChain.fromMoveBatches(moveBatches)
    }

    private fun createRandomMoveBatch(hypotheticalCluster: ModelCluster, size: Int) : MoveActionBatch {
        return buildMoveActionBatch(hypotheticalCluster.makeRandomMoves(size), hypotheticalCluster)
    }

    private fun buildMoveActionBatch(moves: ListIterable<MoveAction>, hypotheticalCluster: ModelCluster): MoveActionBatch {
        val risk = hypotheticalCluster.calculateRisk()
        hypotheticalCluster.stabilizeCluster()

        val score = hypotheticalCluster.calculateBalanceScore()
        val overhead = moves
                .collect { Math.max(it.overhead, balancerConfiguration.minimumShardMovementOverhead) }
                .sum()

        return MoveActionBatch(moves, overhead, risk, score)
    }

    private fun rebalancePreconditionsCheck() : Boolean {
        when {
            routingNodes.count() < 2 ->
                logger.debug("not enough nodes to balance")
            routingNodes.shards { it?.started() == false }.count() > 0 ->
                logger.debug("found non-started or relocating shards, waiting for cluster to stabilize")
            routingNodes.shardsWithState(ShardRoutingState.STARTED).isEmpty() ->
                logger.debug("could not find any started shards to balance")
            balancerConfiguration.concurrentRebalanceSetting == 0 ->
                logger.debug("rebalance disabled")
            initalClusterScore == 0.0 ->
                // this condition can occur during restarts where the cluster services are not
                // fully started yet and thus report "0" size for shards
                logger.debug("cluster score is 0")
            else -> return true
        }
        
        return false
    }

    /**
     * Attempt to allocate unallocated shards using a round-robin scheme
     *
     * Note: This does not leverage random sampling like the rebalance method. Instead, the goal is to get the shards
     *       allocated quickly and then let the rebalance logic move any small shards around.
     */
    fun allocateUnassigned(): BalanceDecision {
        if (!routingNodes.hasUnassignedShards() || routingNodes.count() <= 1) {
            return BalanceDecision.NO_OP
        }

        val unassignedShards = routingNodes.unassigned()
                                           .toList()
                                           .sortedBy { shardSizes[it.shardId()].estimatedSize }
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

        return if (changed) BalanceDecision.BALANCING else BalanceDecision.BALANCED
    }

    /**
     * Attempt to move shards that can no longer be allocated to a node using a round-robin scheme
     *
     * Note: This does not leverage random sampling like the rebalance method. Instead, the goal is to get the shards
     *       allocated quickly and then let the rebalancer logic move any small shards around.
     */
    fun moveShards(): BalanceDecision {
        val shardsThatMustMove = Lists.mutable.ofAll(routingNodes.shards { shouldMove(it!!) })
                                              .sortThisByLong { shardSizes[it.shardId()].estimatedSize }
                                              .reverseThis()
        if (shardsThatMustMove.isEmpty) { return BalanceDecision.NO_OP }

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

        return if (changed) BalanceDecision.BALANCING else BalanceDecision.BALANCED
    }

    private fun shouldMove(shard: ShardRouting) = deciders.canRemain(shard, routingNodes.node(shard.currentNodeId()), allocation).type() == Decision.Type.NO

    private fun tryMove(shard: ShardRouting, nodes: ListIterable<ModelNode>) : ModelNode? = nodes
            .firstOrNull { allocationAllowedForShard(shard, it) && rebalanceAllowedForShard(shard) }
            ?.apply { routingNodes.relocate(shard, this.backingNode.nodeId(), shardSizes[shard.shardId()].estimatedSize) }

    private fun tryAllocation(shard: ShardRouting, nodes: ListIterable<ModelNode>) : ModelNode? = nodes
            .firstOrNull { allocationAllowedForShard(shard, it) }
            ?.apply { routingNodes.initialize(shard, this.backingNode.nodeId(), shardSizes[shard.shardId()].estimatedSize) }

    private fun rebalanceAllowedForShard(shard: ShardRouting) =
            deciders.canRebalance(shard, allocation) != Decision.NO

    private fun allocationAllowedForShard(shard: ShardRouting, it: ModelNode) =
            deciders.canAllocate(shard, it.backingNode, allocation) != Decision.NO
}

enum class BalanceDecision {
    BALANCING,
    ON_HOLD,
    BALANCED,
    NO_OP
}