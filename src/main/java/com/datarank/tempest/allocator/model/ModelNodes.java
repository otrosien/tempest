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

import java.util.*;

public class ModelNodes implements Iterable<ModelNode> {
    private Map<String, ModelNode> nodesById;
    private List<ModelNode> nodesList;
    private Set<ModelShard> assignedShards;
    private long shardsSize;

    public ModelNodes(Random random) {
        nodesById = new HashMap<>();
        nodesList = new RandomList<>(random);
        assignedShards = new HashSet<>();
        shardsSize = 0l;
    }

    public ModelNodes(int numNodes, Random random) {
        nodesById = new HashMap<>(numNodes);
        nodesList = new RandomList<>(numNodes, random);
        assignedShards = new HashSet<>();
        shardsSize = 0l;
    }

    public ModelNodes(ModelNodes other, Random random) {
        nodesById = new HashMap<>();
        nodesList = new RandomList<>(other.getNumNodes(), random);
        assignedShards = new HashSet<>(other.getNumShards());
        shardsSize = 0l;
        // deep copy nodes to prevent shared references
        for (ModelNode node : other.getNodesList()) {
            add(new ModelNode(node, random));
        }
    }

    public void add(ModelNode node) {
        nodesById.put(node.getNodeId(), node);
        nodesList.add(node);
        assignedShards.addAll(node.getShards());
        shardsSize += node.getTotalShardSize();
    }

    public void remove(ModelNode node) {
        nodesById.remove(node.getNodeId());
        nodesList.remove(node);
        assignedShards.removeAll(node.getShards());
        shardsSize -= node.getTotalShardSize();
    }

    public boolean addShard(String nodeId, ModelShard shard) {
        if (!nodesById.containsKey(nodeId)) {
            throw new NoSuchElementException("Cannot find a node with id " + nodeId);
        }
        if(nodesById.get(nodeId).addShard(shard)) {
            assignedShards.add(shard);
            shardsSize += shard.getSize();
            return true;
        }
        return false;
    }

    public boolean removeShard(String nodeId, ModelShard shard) {
        if (!nodesById.containsKey(nodeId)) {
            throw new NoSuchElementException("Cannot find a node with id " + nodeId);
        }
        if(nodesById.get(nodeId).removeShard(shard)) {
            assignedShards.remove(shard);
            shardsSize -= shard.getSize();
            return true;
        }
        return false;
    }

    public ModelShard getShard(int shardId, String nodeId) {
        if (!nodesById.containsKey(nodeId)) {
            throw new NoSuchElementException("Cannot find a node with id " + nodeId);
        }
        ModelNode node = nodesById.get(nodeId);
        for (ModelShard shard : node.getShards()) {
            if (shard.getId() == shardId) {
                return shard;
            }
        }
        return null;
    }

    public boolean nodeContains(String nodeId, ModelShard shard) {
        if (!nodesById.containsKey(nodeId)) {
            throw new NoSuchElementException("Cannot find a node with id " + nodeId);
        }
        return nodesById.get(nodeId).contains(shard);
    }

    public Iterator<ModelNode> iterator() {
        return nodesList.iterator();
    }

    public ModelNode getNode(String nodeId) {
        return nodesById.get(nodeId);
    }

    public List<ModelNode> getNodesSortedBySize() {
        List<ModelNode> sortedNodes = new ArrayList<>(this.nodesList);
        Collections.sort(sortedNodes, new NodeSizeComparator());
        return sortedNodes;
    }

    public ModelNode getLargestNode() {
        ModelNode maxNode = null;
        for (ModelNode node : nodesList) {
            if (maxNode == null || node.getTotalShardSize() > maxNode.getTotalShardSize()) {
                maxNode = node;
            }
        }
        return maxNode;
    }

    public ModelNode getSmallestNode() {
        ModelNode minNode = null;
        for (ModelNode node : nodesList) {
            if (minNode == null || node.getTotalShardSize() < minNode.getTotalShardSize()) {
                minNode = node;
            }
        }
        return minNode;
    }

    public boolean isEmpty() {
        return nodesList.isEmpty();
    }

    public boolean isAssigned(ModelShard shard) {
        return assignedShards.contains(shard);
    }

    public List<ModelNode> getNodesList() { return nodesList; }

    public int size() {
        return nodesById.size();
    }

    public int getNumNodes() { return size(); }

    public int getNumShards() {
        return assignedShards.size();
    }

    public boolean hasShards() {
        return assignedShards.size() > 0;
    }

    public long getShardsSize() {
        return shardsSize;
    }

    class NodeSizeComparator implements Comparator<ModelNode> {
        @Override
        public int compare(ModelNode o1, ModelNode o2) {
            return Long.compare(o1.getTotalShardSize(), o2.getTotalShardSize());
        }
    }
}
