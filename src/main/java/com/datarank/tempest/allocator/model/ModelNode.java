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
import org.elasticsearch.cluster.routing.RoutingNode;

import java.util.List;
import java.util.Random;

public class ModelNode {
    private final RoutingNode routingNode;
    private final String nodeId;
    private List<ModelShard> shards;
    private long totalShardSize;

    public ModelNode(final String nodeId, final Random random){
        this.routingNode = null;
        this.nodeId = nodeId;
        this.shards = new RandomList<>(random);
        this.totalShardSize = 0;
    }

    public ModelNode(final RoutingNode routingNode, final Random random) {
        this.routingNode = routingNode;
        this.nodeId = routingNode.nodeId();
        this.shards = new RandomList<>(routingNode.numberOfOwningShards(), random);
        this.totalShardSize = 0l;
    }

    public ModelNode(final ModelNode other, final Random random) {
        this.routingNode = other.getRoutingNode();
        this.nodeId = other.getNodeId();
        this.shards = new RandomList<>(other.getShards(), random);
        this.totalShardSize = other.getTotalShardSize();
    }

    public boolean addShard(final ModelShard shard) {
        if (shards.add(shard)) {
            totalShardSize += shard.getSize();
            return true;
        }
        return false;
    }

    public boolean removeShard(final ModelShard shard) {
        if (shards.remove(shard)){
            totalShardSize -= shard.getSize();
            return true;
        }
        return false;
    }

    public boolean contains(final ModelShard shard) {
        return shards.contains(shard);
    }

    public long getTotalShardSize() { return this.totalShardSize; }

    public int getNumShards() { return shards.size(); }

    public List<ModelShard> getShards() { return shards; }

    public String getNodeId() {
        return nodeId;
    }

    public RoutingNode getRoutingNode() {
        return routingNode;
    }

}
