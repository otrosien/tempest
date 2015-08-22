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
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;

public class ModelBalancer {
    private ModelCluster currentCluster;
    private ModelCluster bestCluster;

    /**
     * Attempts to find a cluster up to CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE operations away whose
     * energy is lower than the current cluster.
     * @param cluster
     * @return
     */
    public ModelCluster balance(final ModelCluster cluster) {
        int maxIterations = cluster.getSettings().getAsInt(LocalMinimumShardsAllocator.SETTING_MAX_FORKING_ITERATIONS, cluster.getNumNodes() * cluster.getNumShards());
        int maxConcurrentRebalanceOperations = cluster.getSettings().getAsInt(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE, 2);
        maxConcurrentRebalanceOperations = maxConcurrentRebalanceOperations == -1 ? Integer.MAX_VALUE : maxConcurrentRebalanceOperations;

        bestCluster = new ModelCluster(cluster);
        bestCluster.getForkingOperationHistory().clear();
        double bestEnergy = evaluateBalance(bestCluster);
        double currentEnergy;
        for (int i = 0; i < maxIterations; i++) {
            currentCluster = bestCluster.rebalance();
            if (currentCluster == bestCluster) {
                // no further possible moves
                return bestCluster;
            }
            currentEnergy = evaluateBalance(currentCluster);
            if (currentEnergy < bestEnergy) {
                bestCluster = currentCluster;
                bestEnergy = currentEnergy;
            }
            if (bestCluster.getForkingOperationHistory().size() >= maxConcurrentRebalanceOperations) {
                return bestCluster;
            }
        }
        return bestCluster;
    }

    public ModelCluster move(final ModelCluster cluster, final ModelShard shard, final String nodeId) {
        ModelCluster currentCluster = new ModelCluster(cluster);
        currentCluster.clearForkingOperationHistory();
        return currentCluster.move(shard, cluster.getModelNodes().getNode(nodeId));
    }

    /**
     * Evaluates how well a cluster is balanced based on the ratio of the largest node to the smallest. Lower energy
     * implies better balance.
     * @param cluster
     * @return the energy of the cluster
     */
    public static double evaluateBalance(final ModelCluster cluster) {
        if (cluster.getModelNodes().isEmpty()) {
            return 1d; //no data nodes available
        }
        ModelNode maxNode = cluster.getModelNodes().getLargestNode();
        double maxSize = maxNode.getTotalShardSize() != 0 ? (double)maxNode.getTotalShardSize() : 1d;
        ModelNode minNode = cluster.getModelNodes().getSmallestNode();
        double minSize = minNode.getTotalShardSize() != 0 ? (double)minNode.getTotalShardSize() : 1d;
        return maxSize / minSize;
    }
}
