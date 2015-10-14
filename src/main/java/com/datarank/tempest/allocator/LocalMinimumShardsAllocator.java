/* The MIT License (MIT)
 * Copyright (c) 2015 DataRank, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.datarank.tempest.allocator;

import com.datarank.tempest.allocator.model.ModelBalancer;
import com.datarank.tempest.allocator.model.ModelCluster;
import com.datarank.tempest.allocator.model.ModelContainerType;
import com.datarank.tempest.allocator.model.ModelOperation;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.*;

public class LocalMinimumShardsAllocator extends AbstractComponent implements ShardsAllocator {
    public static final Random RANDOM = new Random();
    public static final String SETTING_MAX_MIN_RATIO_THRESHOLD = "cluster.routing.allocation.probabilistic.range_ratio"; // goal maxNode:minNode ratio, default 1.5
    public static final String SETTING_MAX_FORKING_ITERATIONS = "cluster.routing.allocation.probabilistic.iterations"; // max number of attempts to find a better cluster, default numNodes * numShards
    public static final String SETTING_SMALL_SHARD_THRESHOLD = "cluster.routing.allocation.uniform.threshold";
    public static final String SETTING_ALLOW_REBALANCE = "cluster.routing.allocation.allow_rebalance";
    public static final String SETTING_ALLOW_ALLOCATION = "cluster.routing.allocation.allow_allocation";

    @Inject
    public LocalMinimumShardsAllocator(final Settings settings) {
        super(settings);
    }

    @Override
    public void applyStartedShards(StartedRerouteAllocation allocation) {
        /* GATEWAY ONLY */
    }

    @Override
    public void applyFailedShards(FailedRerouteAllocation allocation) {
        /* GATEWAY ONLY */
    }

    /**
     * Attempts to allocate as many unassigned shards as possible.
     *
     * This method is called whenever cluster state changes and there are unassigned shards- a node is created/removed,
     * a shard changes state, or an index is created/destroyed. If some shards cannot be allocated, they are added to
     * ignoredUnassigned, and the next time allocateUnassigned is called, they will be in unassigned and allocation will
     * be reattempted.
     * @param allocation
     * @return
     */
    @Override
    public boolean allocateUnassigned(final RoutingAllocation allocation) {
        allocation.debugDecision(true);
        if (!allocation.routingNodes().hasUnassignedShards()) {
            return false;
        }

        boolean clusterChanged = false;
        logger.info("Allocating unassigned shards.");
        clusterChanged = false;
        ModelCluster currentCluster = new ModelCluster(allocation, settings, RANDOM);
        ModelCluster goalCluster = currentCluster.allocateUnassigned();

        RoutingNodes.UnassignedShards unassignedTransaction = allocation.routingNodes().unassigned().transactionBegin();
        unassignedTransaction.drain();

        Decision decision = canExecute(allocation, goalCluster.getForkingOperationHistory());
        while (decision == Decision.NO || decision == Decision.THROTTLE) {
            logger.warn("Unable to execute allocations, reattempting:" + decision.label());
            goalCluster = currentCluster.allocateUnassigned();
            decision = canExecute(allocation, goalCluster.getForkingOperationHistory());
        }

        for (ModelOperation operation : goalCluster.getForkingOperationHistory()) {
            boolean allocateSuccess = executeOperation(allocation, operation);
            clusterChanged |= allocateSuccess;
            if (!allocateSuccess) {
                logger.error("Failed to execute allocations.");
            }
        }

        allocation.routingNodes().unassigned().transactionEnd(unassignedTransaction);

        if (allocation.routingNodes().ignoredUnassigned() != null && !allocation.routingNodes().ignoredUnassigned().isEmpty()) {
            logger.info("Exiting allocateUnassigned, " + allocation.routingNodes().unassigned().size() + " unassigned shards and " + allocation.routingNodes().ignoredUnassigned().size() + " ignored unassigned shards remaining.");
        }

        return clusterChanged;
    }

    @Override
    public boolean rebalance(final RoutingAllocation allocation) {
        allocation.debugDecision(true);
        logger.info("Rebalancing.");

        if (allocation.routingNodes().hasUnassignedShards() || !allocation.routingNodes().ignoredUnassigned().isEmpty()) {
            logger.info("Detected [{}] unassigned and [{}] ignored unassigned, allocating.", allocation.routingNodes().unassigned().size(), allocation.routingNodes().ignoredUnassigned().size());
            return allocateUnassigned(allocation);
        }

        if (!canRebalance(allocation)) {
            logger.info("Exiting rebalance.");
            return false;
        }

        boolean clusterChanged = false;
        ModelCluster currentCluster = new ModelCluster(allocation, settings);
        double maxMinRatioThreshold = settings.getAsDouble(SETTING_MAX_MIN_RATIO_THRESHOLD, 1.5);
        if (ModelBalancer.evaluateBalance(currentCluster) <= maxMinRatioThreshold) {
            // already sufficiently balanced
            logger.info("Already balanced to " + ModelBalancer.evaluateBalance(currentCluster) + ", exiting rebalance.");
            return clusterChanged;
        }
        logger.info("Attempting to rebalance the cluster.");

        ModelBalancer balancer = new ModelBalancer();
        ModelCluster candidateCluster = balancer.balance(currentCluster, maxMinRatioThreshold);
        Decision decision = canExecute(allocation, candidateCluster.getForkingOperationHistory());

        while (decision != Decision.YES) {
            logger.info("Could not move shards to match cluster: " + decision);
            candidateCluster = balancer.balance(candidateCluster, maxMinRatioThreshold);
            decision = canExecute(allocation, candidateCluster.getForkingOperationHistory());
        }

        if (candidateCluster.getForkingOperationHistory().size() > 0) {
            // try to move to candidateCluster through order-strict operations
            Queue<ModelOperation> operationsToGoalCluster = new LinkedList<>(candidateCluster.getForkingOperationHistory());

            while (!operationsToGoalCluster.isEmpty()){
                ModelOperation operation = operationsToGoalCluster.poll();
                boolean moveSucceeded = executeOperation(allocation, operation);
                clusterChanged |= moveSucceeded;
                if (!moveSucceeded) {
                    logger.warn("Failed to move shard [{}] from node [{}] to node [{}]",
                            InternalClusterInfoService.shardIdentifierFromRouting(operation.modelShard.getRoutingShard()),
                            operation.sourceNode.getNodeId(), operation.destinationNode.getNodeId());
                    break;
                }
            }
        }
        logger.info("Exiting rebalance.");
        return clusterChanged;
    }

    @Override
    public boolean move(final MutableShardRouting shardRouting, final RoutingNode node, final RoutingAllocation allocation) {
        if (node.isEmpty() || !shardRouting.started()) {
            return false;
        }
        logger.info("Attempting to move shard [{}] from [{}]", InternalClusterInfoService.shardIdentifierFromRouting(shardRouting), node.nodeId());
        ModelCluster cluster = new ModelCluster(allocation, settings);
        ModelBalancer balancer = new ModelBalancer();
        ModelCluster candidateCluster = balancer.move(cluster, cluster.getModelNodes().getShard(shardRouting.id(), node.nodeId()), node.nodeId());
        if (candidateCluster.getForkingOperationHistory().size() > 0) {
            return executeOperation(allocation, candidateCluster.getForkingOperationHistory().get(0));
        }
        return false;
    }

    private Decision canExecute(RoutingAllocation allocation, List<ModelOperation> operations) {
        Decision decision;
        for(ModelOperation operation : operations) {
            decision = canExecute(allocation, operation);
            if (decision == Decision.NO || decision == Decision.THROTTLE) {
                return decision;
            }
        }

        return Decision.YES;
    }

    private Decision canExecute(RoutingAllocation allocation, ModelOperation operation) {
        RoutingNode destinationNode = operation.destinationNode.getRoutingNode();
        AllocationDeciders deciders = allocation.deciders();
        ShardRouting shard = operation.modelShard.getRoutingShard();

        if (operation.sourceType == ModelContainerType.UNASSIGNED) {
            Decision clusterDecision = deciders.canAllocate(allocation);
            if (clusterDecision == Decision.NO || clusterDecision == Decision.THROTTLE) {
                return clusterDecision;
            }
            Decision nodeDecision = deciders.canAllocate(destinationNode, allocation);
            if (nodeDecision == Decision.NO || nodeDecision == Decision.THROTTLE) {
                return nodeDecision;
            }


            Decision nodeShardDecision = deciders.canAllocate(shard, destinationNode, allocation);
            if (nodeShardDecision == Decision.NO || nodeShardDecision == Decision.THROTTLE) {
                return nodeShardDecision;
            }
        }
        else {
            if (!shard.started()) {
                return Decision.NO;
            }
            Decision clusterDecision = deciders.canRebalance(shard, allocation);
            if (clusterDecision == Decision.NO || clusterDecision == Decision.THROTTLE) {
                return clusterDecision;
            }

            Decision shardDecision = deciders.canAllocate(shard, destinationNode, allocation);
            if (shardDecision == Decision.NO || shardDecision == Decision.THROTTLE) {
                return shardDecision;
            }
        }

        return Decision.YES;
    }

    private boolean executeOperation(RoutingAllocation allocation, ModelOperation operation) {
        RoutingNode destinationNode = operation.destinationNode.getRoutingNode();
        MutableShardRouting shard = operation.modelShard.getRoutingShard();
        String shardIdentifier = InternalClusterInfoService.shardIdentifierFromRouting(shard);
        RoutingNodes routingNodes = allocation.routingNodes();

        if (operation.sourceType == ModelContainerType.UNASSIGNED) {
            routingNodes.assign(shard, destinationNode.nodeId());
            logger.info("Assigned shard [{}] to node [{}]", shardIdentifier, destinationNode.nodeId());
            return true;
        }
        else {
            if (shard.started()) {
                routingNodes.assign(new MutableShardRouting(shard.index(), shard.id(), destinationNode.nodeId(),
                        shard.currentNodeId(), shard.restoreSource(), shard.primary(), ShardRoutingState.INITIALIZING,
                        shard.version() + 1), destinationNode.nodeId()); // new shard is INITIALIZING
                routingNodes.relocate(shard, destinationNode.nodeId()); // old shard is RELOCATING
                logger.info("Moved shard [{}] from node [{}] to node [{}]", shardIdentifier, operation.sourceNode.getRoutingNode().nodeId(), destinationNode.nodeId());
            }
            else {
                logger.error("Cannot relocate unstarted shard.");
                return false;
            }
        }

        return true;
    }

    private boolean canRebalance(RoutingAllocation allocation) {
        if (settings.getAsInt(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE, 4) == 0 ) {
            logger.info("Allowed concurrent rebalance is <= 0.");
            return false;
        }
        if (!settings.getAsBoolean(SETTING_ALLOW_REBALANCE, true)) {
            logger.info("Rebalancing disabled.");
            return false;
        }
        // batch rebalancing, don't rebalance until all moves finished
        if (allocation.routingNodes().getRelocatingShardCount() > 0) {
            logger.info("[{}] shards currently relocating.");
            return false;
        }
        if (allocation.routingNodes().hasInactiveShards()) {
            logger.info("Cluster currently has inactive shards.");
            return false;
        }
        return true;
    }
}
