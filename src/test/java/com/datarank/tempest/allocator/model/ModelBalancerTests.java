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

import com.datarank.tempest.allocator.LocalMinimumShardsAllocator;
import com.datarank.tempest.allocator.RandomList;
import com.datarank.tempest.allocator.model.*;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class ModelBalancerTests {
    @Mock
    RoutingAllocation allocation;

    @Mock
    ClusterInfo clusterInfo;

    Map<String, Long> shardSizes;
    ModelNodes modelNodes;
    List<ModelShard> unassignedShards;
    List<ModelAllocationDecider> allocationDeciders;
    List<ModelOperation> modelOperations;
    Settings settings;
    Random random;
    ModelBalancer balancer;
    ModelCluster cluster;

    int nodes;
    int shards;

    @Before
    public void setup() {
        clusterInfo = Mockito.mock(ClusterInfo.class);
        allocation = Mockito.mock(RoutingAllocation.class);
        settings = ImmutableSettings.builder()
                .put(LocalMinimumShardsAllocator.SETTING_MAX_MIN_RATIO_THRESHOLD, ByteSizeValue.parseBytesSizeValue("10Gb").getBytes())
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, "ALL")
                .build();

        nodes = 0;
        shards = 0;
        random = new Random(1234);
        shardSizes = new HashMap<>();
        modelNodes = new ModelNodes(20, random);
        unassignedShards = new RandomList<>(100, random);
        allocationDeciders = new ArrayList<>();
        allocationDeciders.add(new ModelSameShardAllocationDecider(settings));
        allocationDeciders.add(new ModelEnableAllocationDecider(settings));
        allocationDeciders.add(new ModelRebalanceOnlyWhenActiveAllocationDecider(settings));
        allocationDeciders.add(new ModelReplicaAfterPrimaryActiveAllocationDecider(settings));
        modelOperations = new ArrayList<>();

        when(clusterInfo.getShardSizes()).thenReturn(shardSizes);
    }

    @Test
    public void productionSizesTest(){
        int numNodes = 17;

        // build nodes
        for (int i = 0; i < numNodes; i++) {
            modelNodes.add(buildNode());
        }

//        double[] shardSizesGb = {10.9, 10.9, 6.8, 6.8, 6.9, 6.9, 9.1, 9.2, 9.2, 9.3, 9.4, 9.4, 6.4, 6.4, 7.0, 7.1, 5.8,
//                5.8, 5.8, 5.8, 8.7, 8.9, 6.0, 6.0, 6.3, 7.0, 15.7, 16.0, 19.6, 19.7, 19.8, 19.9, 19.9, 19.9, 20.0, 20.2,
//                21.8, 21.8, 23.5, 23.6, 25.1, 25.2, 25.3, 25.4, 25.5, 26.0, 26.3, 26.5, 28.0, 28.0, 29.1, 29.1,
//                34.5, 34.6, 34.6, 34.7};

        double[] shardSizesGb = {10.9, 6.8, 6.9, 9.1, 9.2, 9.4, 6.4, 7.0, 5.8, 5.8, 8.7, 6.0, 6.3, 15.7, 19.6, 19.8,
                19.9, 20.0, 21.8, 23.5, 25.1, 25.3, 25.5, 26.3, 28.0, 29.1, 34.5, 34.6};

        // build and add shards
        for (int i = 0; i < shardSizesGb.length; i++) {
            ModelShard shard = buildShard((long)(shardSizesGb[i] * Math.pow(10, 9)));
            ModelShard replica = replicate(shard);
            unassignedShards.add(shard);
            unassignedShards.add(replica);
        }

        cluster = new ModelCluster(clusterInfo, modelNodes, unassignedShards, allocationDeciders, modelOperations, settings, random);
        cluster = cluster.allocateUnassigned();
        balancer = new ModelBalancer();

        ModelCluster bestCluster = cluster;
        int numBalances = 0;
        DateTime start = DateTime.now();
        while(ModelBalancer.evaluateBalance(bestCluster) > 1.5) {
            bestCluster = balancer.balance(bestCluster, 1.5);
            numBalances++;
        }
        long runtimeMs = DateTime.now().getMillis() - start.getMillis();
        double energy = ModelBalancer.evaluateBalance(bestCluster);
        double stddev = ModelCluster.stddev(bestCluster);
        double sizeRangeGb = ModelCluster.sizeRange(bestCluster) / Math.pow(10, 9);
    }

    @Test
    public void productionAssignedTest() {
        int numNodes = 17;

        // build nodes
        for (int i = 1; i <= numNodes; i++) {
            modelNodes.add(new ModelNode(Integer.toString(i), random));
        }

        buildReplicateAdd(33.7, 4, 2);
        buildReplicateAdd(23.8, 2, 1);
        buildReplicateAdd(29.5, 11, 3);
        buildReplicateAdd(20.4, 12, 11);
        buildReplicateAdd(15.3, 12, 8);
        buildReplicateAdd(25.8, 4, 1);
        buildReplicateAdd(22, 8, 13);
        buildReplicateAdd(20.2, 7, 14);
        buildReplicateAdd(25.1, 7, 15);
        buildReplicateAdd(26.7, 5, 14);
        buildReplicateAdd(20.2, 10, 3);
        buildReplicateAdd(29, 16, 5);
        buildReplicateAdd(25.6, 10, 17);
        buildReplicateAdd(21, 9, 6);
        buildReplicateAdd(35.6, 17, 9);
        // non-standard
        buildReplicateAdd(11, 6, 14);
        buildReplicateAdd(6.8, 17, 13);
        buildReplicateAdd(6.8, 15, 4);
        buildReplicateAdd(9.2, 4, 13);
        buildReplicateAdd(9.3, 11, 10);
        buildReplicateAdd(9.5, 7, 12);
        buildReplicateAdd(6.5, 8, 1);
        buildReplicateAdd(7.1, 2, 13);
        buildReplicateAdd(5.8, 9, 5);
        buildReplicateAdd(5.8, 12, 8);
        buildReplicateAdd(8.8, 9, 16);
        buildReplicateAdd(6, 10, 5);
        buildReplicateAdd(6.3, 1, 16);

        cluster = new ModelCluster(clusterInfo, modelNodes, unassignedShards, allocationDeciders, modelOperations, settings, random);
        double startingEnergy = ModelBalancer.evaluateBalance(cluster);
        balancer = new ModelBalancer();

        ModelCluster bestCluster = cluster;
        int numBalances = 0;
        DateTime start = DateTime.now();
        while(ModelBalancer.evaluateBalance(bestCluster) > 1.5) {
            bestCluster = balancer.balance(bestCluster, 1.5);
            numBalances++;
        }
        long runtimeMs = DateTime.now().getMillis() - start.getMillis();
        double energy = ModelBalancer.evaluateBalance(bestCluster);
        double stdDevGb = ModelCluster.stddev(bestCluster) / Math.pow(10, 9);
        double sizeRangeGb = ModelCluster.sizeRange(bestCluster) / Math.pow(10, 9);
    }

    @Test
    public void threeShardsTwoNodesAssignTest() {
        ModelNode node1 = buildNode();
        ModelNode node2 = buildNode();

        modelNodes.add(node1);
        modelNodes.add(node2);

        ModelShard shard1 = buildShard(100);
        ModelShard shard2 = buildShard(100);
        ModelShard shard3 = buildShard(200);

        unassignedShards.add(shard1);
        unassignedShards.add(shard2);
        unassignedShards.add(shard3);

        cluster = new ModelCluster(clusterInfo, modelNodes, unassignedShards, allocationDeciders, modelOperations, settings, random);
        cluster = cluster.allocateUnassigned();
        balancer = new ModelBalancer();

        ModelCluster bestCluster = balancer.balance(cluster, 1.5);

        assertTrue(bestCluster.getUnassignedShards().isEmpty());
        // cluster should have zero energy since there exist (multiple) perfect solutions
        assertThat(ModelBalancer.evaluateBalance(bestCluster), lessThanOrEqualTo(1.5));
    }

    @Test
    public void threeShardsTwoNodesRebalanceTest() {
        ModelNode node1 = buildNode();
        ModelNode node2 = buildNode();

        modelNodes.add(node1);
        modelNodes.add(node2);

        ModelShard shard1 = buildShard(100);
        ModelShard shard2 = buildShard(100);
        ModelShard shard3 = buildShard(200);

        modelNodes.addShard(node1.getNodeId(), shard1);
        modelNodes.addShard(node1.getNodeId(), shard2);
        modelNodes.addShard(node1.getNodeId(), shard3);

        cluster = new ModelCluster(clusterInfo, modelNodes, unassignedShards, allocationDeciders, modelOperations, settings, random);
        balancer = new ModelBalancer();

        ModelCluster bestCluster = balancer.balance(cluster, 1.5);

        assertTrue(bestCluster.getUnassignedShards().isEmpty());
        // cluster should have zero energy since there exist (multiple) perfect solutions
        assertThat(ModelBalancer.evaluateBalance(bestCluster), lessThanOrEqualTo(1.5));
    }

    private ModelNode buildNode() {
        ModelNode node = new ModelNode(Integer.toString(nodes), random);
        nodes++;
        return node;
    }

    private ModelShard buildShard(final long size) {
        ModelShard shard = new ModelShard(shards, size);
        shards++;
        shardSizes.put(Integer.toString(shard.getId()), size);
        return shard;
    }

    private ModelShard replicate(final ModelShard shard) {
        ModelShard replica = buildShard(shard.getSize());
        replica.setIsReplica(true);
        replica.setPrimaryShard(shard);
        return replica;
    }

    private static long giga(final double size) {
        long giga = (long)Math.pow(10, 9);
        return (long)(size * giga);
    }

    private void buildReplicateAdd(final double sizeGb, final int primaryNode, final int replicaNode){
        ModelShard primary = buildShard(giga(sizeGb));
        ModelShard replica = replicate(primary);
        this.modelNodes.addShard(Integer.toString(primaryNode), primary);
        this.modelNodes.addShard(Integer.toString(replicaNode), replica);
    }
}
