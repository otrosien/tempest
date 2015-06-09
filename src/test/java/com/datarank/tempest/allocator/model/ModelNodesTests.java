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

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class ModelNodesTests {
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
    public void addRemoveNodesTest() {
        assertEquals(modelNodes.getNumNodes(), 0);
        assertTrue(modelNodes.isEmpty());

        ModelNode node1 = buildNode();
        modelNodes.add(node1);

        assertEquals(modelNodes.getNumNodes(), 1);
        assertFalse(modelNodes.isEmpty());
        assertEquals(modelNodes.getNode(node1.getNodeId()), node1);

        ModelNode node2 = buildNode();
        modelNodes.add(node2);

        assertEquals(modelNodes.getNumNodes(), 2);
        assertFalse(modelNodes.isEmpty());
        assertEquals(modelNodes.getNode(node2.getNodeId()), node2);

        modelNodes.remove(node1);

        assertEquals(modelNodes.getNumNodes(), 1);
        assertFalse(modelNodes.isEmpty());
        assertEquals(modelNodes.getNode(node1.getNodeId()), null);
        assertEquals(modelNodes.getNode(node2.getNodeId()), node2);
    }

    @Test
    public void addRemoveShardsTest() {
        assertEquals(modelNodes.getNumShards(), 0);

        ModelNode node1 = buildNode();
        modelNodes.add(node1);

        // add shard1 to node1
        ModelShard shard1 = buildShard(DEFAULT_SHARD_SIZE);
        modelNodes.addShard(node1.getNodeId(), shard1);

        assertTrue(modelNodes.nodeContains(node1.getNodeId(), shard1));
        assertTrue(modelNodes.hasShards());
        assertEquals(modelNodes.getNumShards(), 1);
        assertTrue(modelNodes.isAssigned(shard1));

        // add shard2 to node1
        ModelShard shard2 = buildShard(DEFAULT_SHARD_SIZE);
        modelNodes.addShard(node1.getNodeId(), shard2);

        assertTrue(modelNodes.hasShards());
        assertEquals(modelNodes.getNumShards(), 2);
        assertTrue(modelNodes.isAssigned(shard1));
        assertTrue(modelNodes.isAssigned(shard2));

        //add shard3 to node2
        ModelNode node2 = buildNode();
        modelNodes.add(node2);

        ModelShard shard3 = buildShard(DEFAULT_SHARD_SIZE);
        modelNodes.addShard(node2.getNodeId(), shard3);

        assertEquals(modelNodes.getNumShards(), 3);
        assertEquals(modelNodes.getNumNodes(), 2);
        assertTrue(modelNodes.isAssigned(shard1));
        assertTrue(modelNodes.isAssigned(shard2));
        assertTrue(modelNodes.isAssigned(shard3));

        // remove shard1
        modelNodes.removeShard(node1.getNodeId(), shard1);

        assertEquals(modelNodes.getNumShards(), 2);
        assertEquals(modelNodes.getNumNodes(), 2);
        assertFalse(modelNodes.nodeContains(node1.getNodeId(), shard1));
        assertFalse(modelNodes.isAssigned(shard1));
        assertTrue(modelNodes.isAssigned(shard2));
        assertTrue(modelNodes.isAssigned(shard3));

        // remove node1
        modelNodes.remove(node1);

        assertEquals(modelNodes.getNumNodes(), 1);
        assertEquals(modelNodes.getNumShards(), 1);
        assertFalse(modelNodes.isEmpty());
        assertFalse(modelNodes.isAssigned(shard1));
        assertFalse(modelNodes.isAssigned(shard2));
        assertTrue(modelNodes.isAssigned(shard3));
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
