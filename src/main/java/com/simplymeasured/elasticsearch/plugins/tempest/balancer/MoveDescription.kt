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

package com.simplymeasured.elasticsearch.plugins.tempest.balancer

import com.simplymeasured.elasticsearch.plugins.tempest.balancer.model.ModelCluster
import org.elasticsearch.index.shard.ShardId

data class MoveDescription(val sourceNodeId: String, val shardId: ShardId, val destNodeId: String) {
    fun buildMoveAction(hypotheticalCluster: ModelCluster): MoveAction? {
        val localSourceNode = hypotheticalCluster.modelNodes.find { it.nodeId == sourceNodeId } ?: return null
        val localDestNode = hypotheticalCluster.modelNodes.find { it.nodeId == destNodeId } ?: return null
        val localShard = localSourceNode.shards.find { it.backingShard.shardId() == shardId } ?: return null

        return MoveAction(
                sourceNode = localSourceNode.backingNode,
                shard = localShard.backingShard,
                destNode = localDestNode.backingNode,
                overhead = localShard.shardSizeInfo.actualSize,
                shardScoreGroupDescriptions = localShard.scoreGroupDescriptions)
    }
}