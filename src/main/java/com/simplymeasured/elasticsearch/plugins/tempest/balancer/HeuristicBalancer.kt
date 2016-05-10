package com.simplymeasured.elasticsearch.plugins.tempest.balancer

import com.simplymeasured.elasticsearch.plugins.tempest.BalancerState
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.MockDeciders.sameNodeDecider
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.MockDeciders.shardAlreadyMovingDecider
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.MockDeciders.shardIdAlreadyMoving
import org.eclipse.collections.api.block.function.Function2
import org.eclipse.collections.api.block.function.primitive.IntObjectToIntFunction
import org.eclipse.collections.impl.Counter
import org.eclipse.collections.impl.factory.Lists
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

class HeuristicBalancer(    settings: Settings,
                        val allocation: RoutingAllocation,
                        val shardSizeCalculator: ShardSizeCalculator,
                        val balancerState: BalancerState,
                        val random: Random) : AbstractComponent(settings) {

    private val routingNodes: RoutingNodes = allocation.routingNodes()
    private val deciders: AllocationDeciders = allocation.deciders()
    private val mockDeciders: List<MockDecider> = Lists.mutable.of(sameNodeDecider, shardAlreadyMovingDecider, shardIdAlreadyMoving, MockFilterAllocationDecider(settings))
    private val baseModelCluster: ModelCluster = ModelCluster(routingNodes, shardSizeCalculator, mockDeciders, random)
    private val concurrentRebalanceSetting: Int = settings.getAsInt(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE, 4).let { if (it == -1) 4 else it }
    private val searchDepthSetting: Int = settings.getAsInt("tempest.balancer.searchDepth", 8)
    private val searchScaleFactor: Int = settings.getAsInt("tempest.balancer.searchScaleFactor", 1000)
    private val bestNQueueSize: Int = settings.getAsInt("tempest.balancer.searchQueueSize", 10)
    private val minimumShardMovementOverhead: Long = settings.getAsLong("tempest.balancer.minimumShardMovementOverhead", 100000000)
    private val maximumAllowedRiskRate: Double = settings.getAsDouble("tempest.balancer.maximumAllowedRiskRate", 1.10)
    private val forceRebalanceThresholdMinutes: Int = settings.getAsInt("tempest.balancer.forceRebalanceThresholdMinutes", 60)
    private val initalClusterScore: Double = baseModelCluster.calculateBalanceScore()
    private val maximumAllowedRisk: Double = baseModelCluster.calculateRisk() * maximumAllowedRiskRate * maximumAllowedRiskRate

    private var roundRobinAllocatorIndex: Int = 0

    fun rebalance(): Boolean {
        if (!rebalancePreconditionsCheck()) { return false }

        val bestMoveChain = findBestNextMoveChain(baseModelCluster)
        val nextMoveBatch = bestMoveChain.moveBatches.first()

        if (nextMoveBatch.moves.isEmpty()) {
            balancerState.lastStableStructuralHash = baseModelCluster.calculateStructuralHash()
            balancerState.lastFailedRebalanceTimestamp = DateTime.now()
            return false
        }

        for (move in nextMoveBatch.moves) {
            routingNodes.relocate(move.shard.backingShard, move.destNode.nodeId, move.shard.backingShard.expectedShardSize)
        }

        return true
    }

    fun findBestNextMoveChain(modelCluster: ModelCluster) : MoveChain {
        val goodMoveChains = findGoodMoveChains(modelCluster)

        if (goodMoveChains.isEmpty()) {
            return MoveChain.buildOptimalMoveChain(Lists.mutable.of(MoveActionBatch(emptyList<MoveAction>(), 0, 0.0, initalClusterScore)))
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
        return orderedChains.first { isValidBatch(it.moveBatches.first()) }
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

        return bestNQueue.asList().filter { it.risk <= maximumAllowedRisk }
    }

    fun createRandomMoveChain(modelCluster: ModelCluster, maxBatchSize: Int, searchDepth: Int): MoveChain {
        val moveBatches : MutableList<MoveActionBatch> = Lists.mutable.empty()

        for (depth in 1..searchDepth) {
            val nextBatchSize = random.nextInt(maxBatchSize) + 1
            val nextMoveBatch = createRandomMoveBatch(modelCluster, nextBatchSize)
            moveBatches.add(nextMoveBatch)
        }

        return MoveChain.buildOptimalMoveChain(moveBatches)
    }

    fun createRandomMoveBatch(modelCluster: ModelCluster, size: Int) : MoveActionBatch {
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

    fun rebalancePreconditionsCheck() : Boolean {
        if (routingNodes.count() < 2) {
            logger.trace("not enough nodes to balance")
            return false
        }

        if (routingNodes.shards { !it.started() }.count() > 0) {
            logger.trace("found non-started or relocating shards, waiting for cluster to stabilize")
            return false
        }

        if (routingNodes.shardsWithState(ShardRoutingState.STARTED).isEmpty()) {
            logger.trace("could not find any started shards to balance")
            return false
        }

        if (concurrentRebalanceSetting == 0) {
            logger.trace("rebalance disabled")
            return false;
        }

        if (DateTime.now().minusMinutes(forceRebalanceThresholdMinutes).isAfter(balancerState.lastFailedRebalanceTimestamp) ) {
            logger.trace("forcing rebalance due to time threshold expiration")
            return true;
        }

        if (baseModelCluster.calculateStructuralHash() == balancerState.lastStableStructuralHash ) {
            logger.trace("cluster appears to already be balanced")
            return false;
        }

        return true
    }

    fun allocateUnassigned(): Boolean {
        if (!routingNodes.hasUnassignedShards() || routingNodes.count() <= 1) { return false }

        val unassignedShardsIterator = routingNodes.unassigned().iterator()
        var changed = false
        while (unassignedShardsIterator.hasNext()) {
            val assigned = tryAllocation(unassignedShardsIterator.next())

            if (!assigned) {
                unassignedShardsIterator.removeAndIgnore()
            }
            else {
                changed = true
            }
        }

        return changed
    }

    fun moveShards(): Boolean {
        val shardsThatMustMove = Lists.mutable.ofAll(routingNodes.shards { shouldMove(it) }).shuffleThis(random)
        val routingNodesList = routingNodes.toList()
        var changed = false

        for (shard in shardsThatMustMove) {
            val shardSize = allocation.clusterInfo().getShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)

            for (attempt in 1..routingNodesList.size) {
                val node = routingNodesList.get(roundRobinAllocatorIndex++ % routingNodesList.size)

                if (deciders.canRebalance(shard, allocation).type() == Decision.Type.YES &&
                    deciders.canAllocate(shard, node, allocation).type() == Decision.Type.YES) {

                    routingNodes.initialize(shard, node.nodeId(), shardSize)
                    changed = true
                }
            }
        }

        return changed
    }

    private fun shouldMove(shard: ShardRouting) = deciders.canRemain(shard, routingNodes.node(shard.currentNodeId()), allocation).type() == Decision.Type.NO

    private fun tryAllocation(shard: ShardRouting): Boolean {
        val routingNodesList = routingNodes.toList()
        val shardSize = allocation.clusterInfo().getShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)

        for (attempt in 1..routingNodesList.size) {
            val node = routingNodesList.get(roundRobinAllocatorIndex++ % routingNodesList.size)
            val decision = deciders.canAllocate(shard, node, allocation)

            if (decision == Decision.NO) { continue }

            routingNodes.initialize(shard, node.nodeId(), shardSize)
            return true
        }

        logger.warn("failed to find node for unallocated shard: {}", shard)
        return false
    }
}

