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

package com.simplymeasured.elasticsearch.plugins.tempest

import com.simplymeasured.elasticsearch.plugins.tempest.actions.TempestInfoAction
import com.simplymeasured.elasticsearch.plugins.tempest.actions.TempestInfoRequest
import com.simplymeasured.elasticsearch.plugins.tempest.actions.TempestInfoResponse
import com.simplymeasured.elasticsearch.plugins.tempest.actions.TransportTempestInfoAction
import com.simplymeasured.elasticsearch.plugins.tempest.TempestConstants.Companion.EXPUNGE_BLACKLISTED_NODES
import com.simplymeasured.elasticsearch.plugins.tempest.TempestConstants.Companion.FORCE_REBALANCE_THRESHOLD_MINUTES
import com.simplymeasured.elasticsearch.plugins.tempest.TempestConstants.Companion.MAXIMUM_ALLOWED_RISK_RATE
import com.simplymeasured.elasticsearch.plugins.tempest.TempestConstants.Companion.MAXIMUM_SEARCH_TIME_SECONDS
import com.simplymeasured.elasticsearch.plugins.tempest.TempestConstants.Companion.MINIMUM_NODE_SIZE_CHANGE_RATE
import com.simplymeasured.elasticsearch.plugins.tempest.TempestConstants.Companion.MINIMUM_SHARD_MOVEMENT_OVERHEAD
import com.simplymeasured.elasticsearch.plugins.tempest.TempestConstants.Companion.SEARCH_DEPTH
import com.simplymeasured.elasticsearch.plugins.tempest.TempestConstants.Companion.SEARCH_QUEUE_SIZE
import com.simplymeasured.elasticsearch.plugins.tempest.TempestConstants.Companion.SEARCH_SCALE_FACTOR
import org.elasticsearch.action.ActionModule
import org.elasticsearch.cluster.ClusterModule
import org.elasticsearch.cluster.settings.Validator
import org.elasticsearch.plugins.Plugin

/**
 * Main configuration entry point for the Tempest Plugin Required for ES Integration
 */

class TempestPlugin : Plugin() {
    override fun name() = "tempest"

    override fun description() = "shard balancer"

    override fun nodeModules() = mutableListOf(TempestModule())

    fun onModule(clusterModule: ClusterModule) {
        clusterModule.registerShardsAllocator("tempest", TempestShardsAllocator::class.java)
        clusterModule.registerClusterDynamicSetting(SEARCH_DEPTH, Validator.INTEGER)
        clusterModule.registerClusterDynamicSetting(SEARCH_SCALE_FACTOR, Validator.INTEGER)
        clusterModule.registerClusterDynamicSetting(SEARCH_QUEUE_SIZE, Validator.INTEGER)
        clusterModule.registerClusterDynamicSetting(MINIMUM_SHARD_MOVEMENT_OVERHEAD, Validator.INTEGER)
        clusterModule.registerClusterDynamicSetting(MAXIMUM_ALLOWED_RISK_RATE, Validator.DOUBLE)
        clusterModule.registerClusterDynamicSetting(FORCE_REBALANCE_THRESHOLD_MINUTES, Validator.INTEGER)
        clusterModule.registerClusterDynamicSetting(MINIMUM_NODE_SIZE_CHANGE_RATE, Validator.DOUBLE)
        clusterModule.registerClusterDynamicSetting(EXPUNGE_BLACKLISTED_NODES, Validator.BOOLEAN)
        clusterModule.registerClusterDynamicSetting(MAXIMUM_SEARCH_TIME_SECONDS, Validator.INTEGER)
    }

    fun onModule(actionModule: ActionModule) {
        actionModule.registerAction<TempestInfoRequest, TempestInfoResponse>(TempestInfoAction.INSTANCE, TransportTempestInfoAction::class.java)
    }
}

