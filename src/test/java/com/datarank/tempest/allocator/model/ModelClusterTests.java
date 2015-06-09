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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class ModelClusterTests {

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

    int nodes;
    int shards;

    static final long DEFAULT_SHARD_SIZE = 82;

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
        modelOperations = new ArrayList<>();

        when(clusterInfo.getShardSizes()).thenReturn(shardSizes);
    }

    @Test
    public void singleShardSingleNodeForkTest() {
        ModelNode node = buildNode();
        modelNodes.add(node);

        ModelShard shard = buildShard(DEFAULT_SHARD_SIZE);
        unassignedShards.add(shard);

        ModelCluster cluster = new ModelCluster(clusterInfo, modelNodes, unassignedShards, allocationDeciders, modelOperations, settings, random);
        assertEquals(cluster.getUnassignedShards().size(), 1);
        assertEquals(cluster.getNumNodes(), 1);

        ModelCluster forkedCluster = cluster.allocateUnassigned();
        assertEquals(forkedCluster.getNumNodes(), 1);
        assertEquals(forkedCluster.getUnassignedShards().size(), 0);
        assertEquals(forkedCluster.getNumAssignedShards(), 1);
    }

    private ModelNode buildNode() {
        ModelNode node = new ModelNode(Integer.toString(nodes), random);
        nodes++;
        return node;
    }

    private ModelShard buildShard(long size) {
        ModelShard shard = new ModelShard(shards, size);
        shards++;
        shardSizes.put(Integer.toString(shard.getId()), size);
        return shard;
    }
}
