/*
 * The MIT License (MIT)
 * Copyright (c) 2018 DataRank, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 *
 */

package com.simplymeasured.elasticsearch.plugins.tempest.balancer.model

import com.simplymeasured.elasticsearch.plugins.tempest.balancer.IndexSizingGroup
import org.eclipse.collections.api.list.ListIterable
import org.eclipse.collections.api.map.MapIterable
import org.eclipse.collections.impl.utility.LazyIterate
import org.elasticsearch.cluster.routing.RoutingNode
import org.elasticsearch.cluster.routing.ShardRoutingState
import org.elasticsearch.index.shard.ShardId

/**
 * Model representation of a Node and its shards
 */
class ModelNode private constructor(
        val backingNode: RoutingNode,
        val nodeId: String,
        val shardManager: NodeShardManager,
        val isBlacklisted: Boolean,
        val isExpunging: Boolean) {

    val shards: ListIterable<ModelShard>
        get() { return shardManager.shards }

    /**
     * Deep Copy Constructor
     */
    constructor(other: ModelNode) :
        this(other.backingNode,
             other.nodeId,
                NodeShardManager(other.shardManager),
             other.isBlacklisted,
             other.isExpunging)

    /**
     * main constructor used when creating a base model from ES data structures
     */
    constructor(routingNode: RoutingNode,
                shardSizes: MapIterable<ShardId, IndexSizingGroup.ShardSizeInfo>,
                shardScoreGroups: ListIterable<ShardScoreGroup>,
                isBlacklisted: Boolean,
                isExpunging: Boolean) :
        this(backingNode = routingNode,
             nodeId = routingNode.nodeId(),
             shardManager = NodeShardManager(
                     shards = routingNode
                             .copyShards()
                             .let { LazyIterate.adapt(it) }
                             .collect { ModelShard(it, shardSizes[it.shardId()], shardScoreGroups.findScoreGroupDescriptionsForShard(it)) }
                             .toList(),
                     allocationScale = if (isExpunging) 0.0 else
                         routingNode
                                 .node()
                                 .attributes.getOrElse("allocation.scale", { "1.0" })
                                 .toDouble(),
                     shardScoreGroups = shardScoreGroups),
             isBlacklisted = isBlacklisted,
             isExpunging = isExpunging)

    /**
     * sum of all shard sizes
     */
    fun calculateUsage() : Long = shardManager.totalShardSizes

    /**
     * Like calculate usage but weighted in terms of the allocation scale used for this node
     */
    fun calculateNormalizedNodeUsage() : Double = calculateUsage() / shardManager.allocationScale

    /**
     * Remove any relocating shards and set initializing shards to be started
     */
    fun stabilizeNode() {
        shardManager.nonStartedNodes
                .select { it.state == ShardRoutingState.RELOCATING }
                .forEach { shardManager.removeShard(it) }
        shardManager.nonStartedNodes
                .select { it.state == ShardRoutingState.INITIALIZING }
                .forEach { shardManager.updateState(it, ShardRoutingState.STARTED) }
    }
}