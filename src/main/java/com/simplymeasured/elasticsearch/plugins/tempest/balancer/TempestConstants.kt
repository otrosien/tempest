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

package com.simplymeasured.elasticsearch.plugins.tempest.balancer

class TempestConstants {
    companion object {
        val SEARCH_DEPTH = "tempest.balancer.searchDepth"
        val SEARCH_SCALE_FACTOR = "tempest.balancer.searchScaleFactor"
        val SEARCH_QUEUE_SIZE = "tempest.balancer.searchQueueSize"
        val MINIMUM_SHARD_MOVEMENT_OVERHEAD = "tempest.balancer.minimumShardMovementOverhead"
        val MAXIMUM_ALLOWED_RISK_RATE = "tempest.balancer.maximumAllowedRiskRate"
        val FORCE_REBALANCE_THRESHOLD_MINUTES = "tempest.balancer.forceRebalanceThresholdMinutes"
        val MINIMUM_NODE_SIZE_CHANGE_RATE = "tempest.balancer.minimumNodeSizeChangeRate"
        val EXPUNGE_BLACKLISTED_NODES = "tempest.balancer.expungeBlacklistedNodes"
    }
}