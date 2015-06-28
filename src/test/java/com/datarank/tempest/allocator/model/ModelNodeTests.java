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
import com.datarank.tempest.allocator.model.ModelAllocationDecider;
import com.datarank.tempest.allocator.model.ModelNode;
import com.datarank.tempest.allocator.model.ModelNodes;
import com.datarank.tempest.allocator.model.ModelShard;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class ModelNodeTests {
    @Mock
    RoutingAllocation allocation;

    @Mock
    ClusterInfo clusterInfo;

    Map<String, Long> shardSizes;
    ModelNodes modelNodes;
    List<ModelShard> unassignedShards;
    Set<ModelShard> ignoredUnassignedShards;
    List<ModelAllocationDecider> allocationDeciders;
    Random random;

    int nodes;
    int shards;

    static final long DEFAULT_SHARD_SIZE = 82;

    @Before
    public void setup() {
        clusterInfo = Mockito.mock(ClusterInfo.class);
        nodes = 0;
        shards = 0;
        random = new Random(1234);
        shardSizes = new HashMap<>();
        modelNodes = new ModelNodes(20, random);
        unassignedShards = new RandomList<>(100, random);
        ignoredUnassignedShards = new HashSet<>();
        allocationDeciders = new ArrayList<>();

        when(clusterInfo.getShardSizes()).thenReturn(shardSizes);
    }

    @Test
    public void addRemoveShardsTest() {
        ModelNode node = buildNode();

        assertEquals(node.getNumShards(), 0);
        assertEquals(node.getTotalShardSize(), 0l);
        assertTrue(node.getShards().isEmpty());

        ModelShard shard1 = buildShard(DEFAULT_SHARD_SIZE);

        node.addShard(shard1);

        assertTrue(node.contains(shard1));
        assertEquals(node.getNumShards(), 1);
        assertEquals(node.getTotalShardSize(), DEFAULT_SHARD_SIZE);

        ModelShard shard2 = buildShard(DEFAULT_SHARD_SIZE);

        node.addShard(shard2);

        assertTrue(node.contains(shard2));
        assertEquals(node.getNumShards(), 2);
        assertEquals(node.getTotalShardSize(), 2 * DEFAULT_SHARD_SIZE);

        node.removeShard(shard1);
        node.removeShard(shard2);

        assertEquals(node.getNumShards(), 0);
        assertEquals(node.getTotalShardSize(), 0);
        assertTrue(node.getShards().isEmpty());
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

}
