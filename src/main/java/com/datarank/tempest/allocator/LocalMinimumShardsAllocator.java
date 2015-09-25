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
import com.datarank.tempest.allocator.model.ModelOperation;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRoutingState;
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
import org.elasticsearch.common.settings.SettingsException;

import java.util.*;

public class LocalMinimumShardsAllocator extends AbstractComponent implements ShardsAllocator {
    public static final Random RANDOM = new Random();
    public static final String SETTING_MAX_MIN_RATIO_THRESHOLD = "cluster.routing.allocation.probabilistic.range_ratio"; // goal maxNode:minNode ratio, default 1.5
    public static final String SETTING_MAX_FORKING_ITERATIONS = "cluster.routing.allocation.probabilistic.iterations"; // max number of attempts to find a better cluster, default numNodes * numShards
    public static final String SETTING_SMALL_SHARD_THRESHOLD = "cluster.routing.allocation.uniform.threshold";

    private RoutingNodes routingNodes;
    private List<MutableShardRouting> ignoredUnassigned;
    private AllocationDeciders deciders;
    private MutableShardRouting shard;
    private RoutingNode destinationNode;
    private Decision nodeDecision;
    private Decision shardDecision;
    private Decision nodeShardDecision;


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
        if (!allocation.routingNodes().hasUnassignedShards()) {
            return false;
        }

        boolean clusterChanged = false;
        try {
            logger.info("Allocating unassigned shards.");
            clusterChanged = false;
            ModelCluster currentCluster = new ModelCluster(allocation, settings, RANDOM);
            ModelCluster goalCluster = currentCluster.allocateUnassigned();

            RoutingNodes.UnassignedShards unassignedTransaction = allocation.routingNodes().unassigned().transactionBegin();
            Set<MutableShardRouting> unassignedShards = new HashSet<>(Arrays.asList(unassignedTransaction.drain()));

            for (ModelOperation operation : goalCluster.getForkingOperationHistory()) {
                // except for replica-primary allocation decisions, unassigned allocation is not order-strict
                clusterChanged |= tryAllocateOperation(allocation, operation, unassignedShards);
            }

            // model may not have allocated all shards, put them in ignoredUnassigned
            for (MutableShardRouting shard : unassignedShards) {
                allocation.routingNodes().ignoredUnassigned().add(shard);
            }

            allocation.routingNodes().unassigned().transactionEnd(unassignedTransaction);
        } catch (Exception e) {
            logger.warn(e.getStackTrace().toString());
        }

        if (allocation.routingNodes().ignoredUnassigned() != null && !allocation.routingNodes().ignoredUnassigned().isEmpty()) {
            logger.info("Exiting allocateUnassigned, " + allocation.routingNodes().unassigned().size() + " unassigned shards and " + allocation.routingNodes().ignoredUnassigned().size() + " ignored unassigned shards remaining.");
        }

        return clusterChanged;
    }

    @Override
    public boolean rebalance(final RoutingAllocation allocation) {
        logger.info("Rebalancing.");

        if (!canRebalance(allocation)) {
            return false;
        }

        boolean clusterChanged = false;
        try {
            ModelCluster currentCluster = new ModelCluster(allocation, settings);
            double maxMinRatioThreshold = settings.getAsDouble(SETTING_MAX_MIN_RATIO_THRESHOLD, 1.5);
            if (ModelBalancer.evaluateBalance(currentCluster) <= maxMinRatioThreshold) {
                // already sufficiently balanced
                logger.info("[{}] unassigned, [{}] ignored unassigned.", allocation.routingNodes().unassigned().size(), allocation.routingNodes().ignoredUnassigned().size());
                logger.info("Already balanced to " + ModelBalancer.evaluateBalance(currentCluster) + ", exiting rebalance.");
                return clusterChanged;
            }
            logger.info("Attempting to rebalance the cluster.");

            ModelBalancer balancer = new ModelBalancer();
            ModelCluster candidateCluster = balancer.balance(currentCluster);
            if (candidateCluster.getForkingOperationHistory().size() > 0) {
                // try to move to candidateCluster through order-strict operations
                Queue<ModelOperation> operationsToGoalCluster = new LinkedList<>(candidateCluster.getForkingOperationHistory());

                while (!operationsToGoalCluster.isEmpty()){
                    boolean moveSucceeded = tryRebalanceOperation(allocation, operationsToGoalCluster.poll());
                    clusterChanged |= moveSucceeded;
                    if (!moveSucceeded) {
                        break;
                    }
                }
            }
        } catch (SettingsException e) {
            logger.warn(e.getStackTrace().toString());
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
            return tryRebalanceOperation(allocation, candidateCluster.getForkingOperationHistory().get(0));
        }
        return false;
    }

    private void initialize(RoutingAllocation allocation, ModelOperation operation) {
        routingNodes = allocation.routingNodes();
        ignoredUnassigned = routingNodes.ignoredUnassigned();
        deciders = allocation.deciders();
        shard = operation.modelShard.getRoutingShard();
        destinationNode = operation.destinationNode.getRoutingNode();
        nodeDecision = deciders.canAllocate(destinationNode, allocation);
        shardDecision = deciders.canAllocate(shard, allocation);
        nodeShardDecision = deciders.canAllocate(shard, destinationNode, allocation);
    }

    private boolean tryAllocateOperation(final RoutingAllocation allocation, final ModelOperation operation, final Set<MutableShardRouting> unassignedShards) {
        initialize(allocation, operation);
        String shardIdentifier = InternalClusterInfoService.shardIdentifierFromRouting(operation.modelShard.getRoutingShard());

        if (shardDecision.type() != Decision.Type.YES) {
            logger.info("Unable to allocate " + shardIdentifier + ", ignoring.");
            ignoredUnassigned.add(shard);
            unassignedShards.remove(shard);
            return false;
        }

        unassignedShards.remove(shard);
        routingNodes.assign(shard, destinationNode.nodeId());
        logger.info("Assigned shard [{}] to node [{}]", shardIdentifier, destinationNode.nodeId());
        return true;
    }

    private boolean tryRebalanceOperation(final RoutingAllocation allocation, final ModelOperation operation) {
        initialize(allocation, operation);

        if (nodeDecision.type() != Decision.Type.YES || shardDecision.type() != Decision.Type.YES || nodeShardDecision.type() != Decision.Type.YES) {
            logger.info("Unable to move shard " + InternalClusterInfoService.shardIdentifierFromRouting(operation.modelShard.getRoutingShard()));
            return false;
        }
        if (shard.started()) {
            routingNodes.assign(new MutableShardRouting(shard.index(), shard.id(), destinationNode.nodeId(),
                    shard.currentNodeId(), shard.restoreSource(), shard.primary(), ShardRoutingState.INITIALIZING,
                    shard.version() + 1), destinationNode.nodeId()); // new shard is INITIALIZING
            routingNodes.relocate(shard, destinationNode.nodeId()); // old shard is RELOCATING
            logger.info("Moved shard [{}] from node [{}] to node [{}]", InternalClusterInfoService.shardIdentifierFromRouting(shard), operation.sourceNode.getRoutingNode().nodeId(), destinationNode.nodeId());
        }
        
        return true;
    }

    private boolean canRebalance(RoutingAllocation allocation) {
        if (settings.getAsInt(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE, 4) <= 0 ) {
            logger.info("Allowed concurrent rebalance is <= 0, exiting rebalance.");
            return false;
        }
        if (allocation.routingNodes().hasUnassignedShards() || !allocation.routingNodes().ignoredUnassigned().isEmpty()) {
            logger.info(allocation.routingNodes().unassigned().size() + " unassigned shards and " + allocation.routingNodes().ignoredUnassigned().size() + " ignored unassigned shards remain, exiting rebalance.");
            return false;
        }
        return true;
    }
}
