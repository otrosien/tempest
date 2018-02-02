/*
 * The MIT License (MIT)
 * Copyright (c) 2016 DataRank, Inc.
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

package com.simplymeasured.elasticsearch.plugins.tempest.balancer

import com.simplymeasured.elasticsearch.plugins.tempest.balancer.model.ShardScoreGroupDescription
import org.eclipse.collections.api.list.ListIterable
import org.elasticsearch.cluster.routing.RoutingNode
import org.elasticsearch.cluster.routing.ShardRouting

/**
 * Simulated Move that holds the source and destination model node and shard
 */
data class MoveAction(
        val sourceNode: RoutingNode,
        val shard: ShardRouting,
        val destNode: RoutingNode,
        val overhead: Long,
        val shardScoreGroupDescriptions: ListIterable<ShardScoreGroupDescription>) {

    fun buildMoveDescription(): MoveDescription = MoveDescription(
            sourceNodeId = sourceNode.nodeId(),
            shardId = shard.shardId(),
            destNodeId = destNode.nodeId())
}