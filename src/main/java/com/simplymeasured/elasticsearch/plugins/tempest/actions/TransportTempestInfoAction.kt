/*
 * The MIT License (MIT)
 * Copyright (c) 2017 DataRank, Inc.
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

package com.simplymeasured.elasticsearch.plugins.tempest.actions

import com.simplymeasured.elasticsearch.plugins.tempest.TempestShardsAllocator
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.IndexGroupPartitioner
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.ShardSizeCalculator
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction
import org.elasticsearch.cluster.ClusterInfoService
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService

/**
 * TransportAction that provides stats about tempest
 */
class TransportTempestInfoAction
    @Inject constructor(settings: Settings,
                        actionName: String,
                        transportService: TransportService,
                        clusterService: ClusterService,
                        val clusterInfoService: ClusterInfoService,
                        val tempestAllocator: TempestShardsAllocator,
                        val indexGroupPartitioner: IndexGroupPartitioner,
                        val shardSizeCalculator: ShardSizeCalculator,
                        threadPool: ThreadPool,
                        actionFilters: ActionFilters,
                        indexNameExpressionResolver: IndexNameExpressionResolver):
        TransportMasterNodeReadAction<TempestInfoRequest, TempestInfoResponse>(
                settings,
                actionName,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                indexNameExpressionResolver,
                ::TempestInfoRequest) {

    override fun executor(): String = ThreadPool.Names.SAME

    override fun checkBlock(request: TempestInfoRequest?, state: ClusterState?): ClusterBlockException? = null

    override fun newResponse(): TempestInfoResponse = TempestInfoResponse()

    override fun masterOperation(request: TempestInfoRequest, state: ClusterState, listener: ActionListener<TempestInfoResponse>) {
        val response = TempestInfoResponse()

        response.patternMapping = indexGroupPartitioner.patternMapping(state.metaData())
        response.youngIndexes = shardSizeCalculator.youngIndexes(state.metaData())
        response.lastBalanceChangeDateTime = tempestAllocator.lastBalanceChangeDateTime
        response.lastOptimalBalanceFoundDateTime = tempestAllocator.lastOptimalBalanceFoundDateTime
        response.lastRebalanceAttemptDateTime = tempestAllocator.lastRebalanceAttemptDateTime
        response.status = tempestAllocator.status
        response.lastNodeGroupScores = tempestAllocator.lastNodeGroupScores
        listener.onResponse(response)
    }

}