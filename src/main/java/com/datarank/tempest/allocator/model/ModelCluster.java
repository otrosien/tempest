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
package com.datarank.tempest.allocator.model;

import com.datarank.tempest.allocator.RandomList;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;

import java.util.*;

/**
 * Represents a possible state of the cluster. Forking is done by new instance creation, rather than in place. In place
 * might be faster, but it would be a lot more likely to run into validity issues, particularly with AllocationDeciders.
 */
public class ModelCluster {
    public static Random RANDOM = new Random();

    private static final Predicate<MutableShardRouting> assignedPredicate = new Predicate<MutableShardRouting>() {
        @Override
        public boolean apply(MutableShardRouting input) {
            return input.assignedToNode();
        }
    };
    private final RoutingAllocation routingAllocation;
    private final ClusterInfo clusterInfo;
    private final Settings settings;
    private final ModelCluster startingCluster;

    private ModelNodes modelNodes;
    // partitioning shards to avoid the expense of filtering before random selection from the collection of shards
    private List<ModelShard> unassignedShards;
    private List<ModelAllocationDecider> allocationDeciders;
    private List<ModelOperation> forkingOperationHistory;

    //region Constructors
    //TODO:SCA: remove testing constructor and mock out RoutingAllocation
    public ModelCluster(final ClusterInfo clusterInfo, final ModelNodes modelNodes,
                        final List<ModelShard> unassignedShards, final List<ModelAllocationDecider> allocationDeciders,
                        List<ModelOperation> forkingOperationHistory, Settings settings, Random random) {
        this.clusterInfo = clusterInfo;
        this.modelNodes = modelNodes;
        this.unassignedShards = unassignedShards;
        this.allocationDeciders = allocationDeciders;
        this.forkingOperationHistory = forkingOperationHistory;
        this.startingCluster = this;
        this.settings = settings;
        RANDOM = random;

        routingAllocation = null;
    }

    /**
     * Copy constructor, shallow copies all collection elements except for modelNode_s.
     * @param other
     */
    public ModelCluster(final ModelCluster other) {
        this.routingAllocation = other.getRoutingAllocation();
        this.clusterInfo = other.getClusterInfo();
        // modelNodes need to be deep copied to prevent accidental modification of a cluster by its copy
        this.modelNodes = new ModelNodes(other.getModelNodes(), RANDOM);
        // each time we fork, move all ignoredUnassigned shards back to unassigned
        this.unassignedShards = new RandomList<>(other.getUnassignedShards(), RANDOM);
        this.forkingOperationHistory = new ArrayList<>(other.getForkingOperationHistory());
        this.settings = other.getSettings();
        this.allocationDeciders = other.getAllocationDeciders();
        this.startingCluster = other.getStartingCluster();
    }

    public ModelCluster(final RoutingAllocation allocation, final Settings settings) {
        this.routingAllocation = allocation;
        this.clusterInfo = allocation.clusterInfo();
        this.forkingOperationHistory = new ArrayList<>();
        this.settings = settings;
        this.startingCluster = this;

        buildModel();
    }

    public ModelCluster(final RoutingAllocation allocation, final Settings settings, final Random random) {
        this.routingAllocation = allocation;
        this.clusterInfo = allocation.clusterInfo();
        this.forkingOperationHistory = new ArrayList<>();
        this.settings = settings;
        this.startingCluster = this;
        RANDOM = random;

        buildModel();
    }
    //endregion

    //region Forking

    /**
     * Attempts to allocate all unassigned shards in the cluster.
     *
     * Note that because this method randomly chooses an
     * unassigned shard to allocate and the node to allocate it on, and because the ability of a shard to be allocated
     * on a node is effected by previous allocations, it is possible that there exists one or more valid end clusters
     * where all shards are allocated, but that this method will not find one of them. The probability of finding a
     * valid end cluster state increases linearly with the number of valid end cluster states. The following will all
     * increase the probability of finding a valid end cluster state:
     * - Fewer routing filters/awareness restrictions. If none, this method is VERY likely to find a valid end cluster state.
     * - More nodes
     * - Fewer replicas
     *
     * @return A cluster with as many shards allocated as possible
     */
    public ModelCluster allocateUnassigned() {
        if (!this.hasUnassignedShards()) {
            return null;
        }
        ModelCluster forkedCluster = this;
        ModelCluster nextCluster;
        while (forkedCluster.hasUnassignedShards()) {
            nextCluster = forkedCluster.forkUnassigned();
            if (nextCluster == forkedCluster) {
                return forkedCluster;
            }
            forkedCluster = nextCluster;
        }
        return forkedCluster;
    }

    public ModelCluster rebalance() {
        if (this.hasUnassignedShards()) {
            throw new IllegalStateException("Cannot rebalance while there are still unassigned shards.");
        }
        if (!modelNodes.hasShards()) {
            throw new IllegalStateException("Cannot relocate a random shard if there are no shards to relocate.");
        }
        return forkAssigned();
    }

    public ModelCluster move(final ModelShard shard, final ModelNode sourceNode) {
        if (!modelNodes.isAssigned(shard) || !modelNodes.nodeContains(sourceNode.getNodeId(), shard)) {
            return this;
        }
        ModelOperation operation = findValidRelocation(shard, sourceNode);
        // no possible moves for this shard, leave it where it is
        if (operation == ModelOperation.NO_OP) {
            return this;
        }
        return forkCluster(this, operation);
    }

    /**
     * Try to fork the cluster by moving unassigned shards to a node
     * @return a new ModelCluster with the move performed, or null if no such move is possible
     */
    private ModelCluster forkUnassigned() {
        if (unassignedShards.isEmpty()) { return null; }
        ModelOperation operation;
        for (ModelShard unassignedShard : unassignedShards) {
            operation = findValidRelocation(unassignedShard);
            if (operation == ModelOperation.NO_OP) { continue; }
            return forkCluster(this, operation);
        }
        return this;
    }

    /**
     * Try to fork the cluster by moving a shard that is already assigned to a node
     * @return a new ModelCluster with the move performed, or null if no such move is possible
     */
    private ModelCluster forkAssigned() {
        ModelOperation operation;
        for (ModelNode sourceNode : modelNodes) {
            if (sourceNode.getShards().isEmpty()) { continue; }
            for (ModelShard shard : sourceNode.getShards()) {
                operation = tryMoveToNode(shard, sourceNode);
                // no possible moves for this shard, leave it where it is
                if (operation == ModelOperation.NO_OP) { continue; }
                return forkCluster(this, operation);
            }
        }
        return this;
    }

    /**
     * Forks sourceCluster by copying it and executing operation on the new cluster. To avoid redundancy,
     * no validation checks are performed on operation.
     * @param sourceCluster
     * @param operation
     * @return a new ModelCluster representing sourceCluster modified by operation.
     */
    private static ModelCluster forkCluster(final ModelCluster sourceCluster, final ModelOperation operation) {
        ModelCluster forkedCluster = new ModelCluster(sourceCluster);
        if (!forkedCluster.executeOperation(operation)) {
            throw new RuntimeException("Failed to execute operation on the cluster.");
        }
        return forkedCluster;
    }

    /**
     * Attempts to execute an operation on the current ModelCluster.
     * @param operation
     * @return true if the operation was successful, false otherwise.
     */
    public boolean executeOperation(final ModelOperation operation){
        if (operation == ModelOperation.NO_OP) {
            return true;
        }
        switch (operation.sourceType) {
            case UNASSIGNED:
                if (!unassignedShards.remove(operation.modelShard)) { return false; }
                if (!modelNodes.addShard(operation.destinationNode.getNodeId(), operation.modelShard)) { return false; }
                break;
            case NODE:
                if (!modelNodes.removeShard(operation.sourceNode.getNodeId(), operation.modelShard)) { return false; }
                if (!modelNodes.addShard(operation.destinationNode.getNodeId(), operation.modelShard)) { return false; }
                break;
            default:
                throw new IllegalStateException("Invalid sourceType " + operation.sourceType + " for operation.");
        }
        return this.forkingOperationHistory.add(operation);
    }

    /**
     * Determines if an operation is valid given the current ModelCluster. Provides some verification that the state is
     * valid and stable.
     * @param operation
     * @return
     */
    private boolean canExecute(final ModelOperation operation) {
        if (operation == ModelOperation.NO_OP) {
            return true;
        }

        ModelShard shard = operation.modelShard;
        if (modelNodes.nodeContains(operation.destinationNode.getNodeId(), shard)) { return false; }
        switch (operation.sourceType) {
            case UNASSIGNED:
                if (!unassignedShards.contains(shard)) { return false; }
                if (modelNodes.isAssigned(shard)) { return false; }
                break;
            case NODE:
                if (unassignedShards.contains(shard)) { return false; }
                if (!modelNodes.nodeContains(operation.sourceNode.getNodeId(), shard)) { return false; }
                if (modelNodes.nodeContains(operation.destinationNode.getNodeId(), shard)) { return false; }
                break;
            default:
                throw new IllegalStateException("Invalid sourceType " + operation.sourceType + " for operation.");
        }
        for (ModelAllocationDecider allocationDecider : allocationDeciders) {
           if (!allocationDecider.canRelocate(this, operation)) { return false; }
        }
        return true;
    }

    /**
     * Finds the smallest node that the unassigned shard can be moved to.
     * @param shard
     * @return a ModelOperation representing a move from UNASSIGNED to the destination node.
     */
    private ModelOperation findValidRelocation(final ModelShard shard) {
        ModelOperation operation;
        for (ModelNode destinationNode : modelNodes.getNodesSortedBySize()) {
            operation = new ModelOperation(shard, null, destinationNode);
            if (ModelOperation.isValid(operation) && canExecute(operation)) {
                return operation;
            }
        }
        return ModelOperation.NO_OP;
    }

    /**
     * Randomly finds a possible destination for a shard.
     *
     * Unassigned shards can be moved to a node or, if not possible, to ignoredUnassigned.
     * Shards on nodes (assigned) can be moved to another node. If not possible, ModelOperation.NO_OP is returned,
     * representing that the shard must stay in its current location.
     * @param shard
     * @return a ModelOperation representing a move from the shard's current location to a destination node
     */
    private ModelOperation findValidRelocation(final ModelShard shard, final ModelNode sourceNode) {
        ModelOperation operation = tryMoveToNode(shard, sourceNode);
        return operation;
    }

    private ModelOperation tryMoveToNode(final ModelShard shard, final ModelNode sourceNode) {
        ModelOperation operation;
        for (ModelNode destinationNode : modelNodes) {
            operation = new ModelOperation(shard, sourceNode, destinationNode);
            if (ModelOperation.isValid(operation) && canExecute(operation)) {
                return operation;
            }
        }
        return ModelOperation.NO_OP;
    }
    //endregion

    //region ElasticSearch Integration
    /**
     * Builds a complete model (cluster, nodes, and shards) from this.routingAllocation. Should only be called before
     * operating on the cluster.
     */
    private void buildModel() {
        this.modelNodes = new ModelNodes(RANDOM);
        this.unassignedShards = new RandomList<>(routingAllocation.routingNodes().unassigned().size(), RANDOM);
        this.allocationDeciders = buildDefaultAllocationDeciders(this.settings);
        // build ModelNodes
        for (RoutingNode routingNode : routingAllocation.routingNodes()) {
            modelNodes.add(new ModelNode(routingNode, RANDOM));
        }
        /* There's no simple way to get only primary/replica shards, and there's also no simple way to get the primary
           that corresponds to a replica. So build and add shards where they should go, then set primaries.
         */
        Map<ShardId, ModelShard> primaryShards = new HashMap<>();
        Map<ShardId, ModelShard> replicaShards = new HashMap<>();
        for (MutableShardRouting routingShard : routingAllocation.routingNodes().shards(assignedPredicate)){
            ModelShard shard = new ModelShard(routingShard, getShardSize(routingShard));
            if (routingShard.primary()){
                primaryShards.put(shard.getShardIdentifier(), shard);
                shard.setPrimaryShard(shard);
            }
            else {
                replicaShards.put(shard.getShardIdentifier(), shard);
            }
            modelNodes.addShard(routingShard.currentNodeId(), shard);
        }
        for (MutableShardRouting routingShard : routingAllocation.routingNodes().unassigned()) {
            ModelShard shard = new ModelShard(routingShard, getShardSize(routingShard));
            if (routingShard.primary()){
                primaryShards.put(shard.getShardIdentifier(), shard);
                shard.setPrimaryShard(shard);
            }
            else {
                replicaShards.put(shard.getShardIdentifier(), shard);
            }
            unassignedShards.add(shard);
        }

        for (MutableShardRouting routingShard : routingAllocation.routingNodes().ignoredUnassigned()) {
            ModelShard shard = new ModelShard(routingShard, getShardSize(routingShard));
            if (routingShard.primary()) {
                primaryShards.put(shard.getShardIdentifier(), shard);
                shard.setPrimaryShard(shard);
            }
            else {
                replicaShards.put(shard.getShardIdentifier(), shard);
            }
            unassignedShards.add(shard);
        }

        // set primaries
        for (ModelShard replica : replicaShards.values()) {
            replica.setPrimaryShard(primaryShards.get(replica.getShardIdentifier())); // replicas and primaries share ids
        }
    }

    private List<ModelAllocationDecider> buildDefaultAllocationDeciders(final Settings settings) {
        List<ModelAllocationDecider> deciders = new ArrayList<>();
        deciders.add(new ModelSameShardAllocationDecider(settings));
        deciders.add(new ModelEnableAllocationDecider(settings));
        deciders.add(new ModelRebalanceOnlyWhenActiveAllocationDecider(settings));
        deciders.add(new ModelReplicaAfterPrimaryActiveAllocationDecider(settings));
        return deciders;
    }
    //endregion

    //region Calculations and Sizes
    public int getNumNodes() {
        return modelNodes.size();
    }

    public int getNumShards() { return getNumAssignedShards() + getNumUnassignedShards(); }

    public int getNumAssignedShards() {
        return modelNodes.getNumShards();
    }

    public int getNumUnassignedShards() { return unassignedShards.size(); }

    public long getTotalShardsSize() {
        return modelNodes.getShardsSize() + calculateTotalSizeOfShards(unassignedShards);
    }

    private long calculateTotalSizeOfShards(final Collection<ModelShard> shards) {
        long size = 0l;
        for (ModelShard shard : shards) {
            size += shard.getSize();
        }
        return size;
    }

    private long getShardSize(final MutableShardRouting routingShard) {
        String key = InternalClusterInfoService.shardIdentifierFromRouting(routingShard);

        if (!clusterInfo.getShardSizes().containsKey(key)) {
            return ModelShard.UNKNOWN_SHARD_SIZE;
        }

        return clusterInfo.getShardSizes().get(key);
    }

    public static double stddev(final ModelCluster cluster) {
        double mean = cluster.getTotalShardsSize() / cluster.getNumNodes();
        double variance = 0d;

        for (ModelNode node : cluster.getModelNodes()) {
            double var = node.getTotalShardSize() - mean;
            variance += var * var;
        }
        return Math.sqrt(variance / cluster.getNumNodes());
    }

    /**
     * Calculates the difference between the largest node and the smallest node
     * @param cluster
     * @return
     */
    public static long sizeRange(final ModelCluster cluster) {
        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;
        long size;
        for (ModelNode node : cluster.getModelNodes()) {
            size = node.getTotalShardSize();
            if (size > max) { max = size; }
            if (size < min) { min = size; }
        }
        return max - min;
    }
    //endregion

    //region Accessors
    public RoutingAllocation getRoutingAllocation() {
        return routingAllocation;
    }

    public ClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    public ModelNodes getModelNodes() {
        return modelNodes;
    }

    public List<ModelShard> getUnassignedShards() { return unassignedShards; }

    public boolean hasUnassignedShards() { return !unassignedShards.isEmpty(); }

    public List<ModelAllocationDecider> getAllocationDeciders() { return allocationDeciders; }

    public List<ModelOperation> getForkingOperationHistory() { return forkingOperationHistory; }

    public ModelCluster getStartingCluster() {
        return startingCluster;
    }

    public Settings getSettings() {
        return settings;
    }
    //endregion

    public void clearForkingOperationHistory() {
        forkingOperationHistory.clear();
    }
}
