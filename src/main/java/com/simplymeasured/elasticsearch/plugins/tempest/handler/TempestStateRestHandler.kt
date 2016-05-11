package com.simplymeasured.elasticsearch.plugins.tempest.handler

import com.simplymeasured.elasticsearch.plugins.tempest.BalancerState
import com.simplymeasured.elasticsearch.plugins.tempest.TempestShardsAllocator
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.ModelCluster
import org.eclipse.collections.api.RichIterable
import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.inject.Provider
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.rest.*
import org.joda.time.DateTime

/**
 * Created by awhite on 4/14/16.
 */
class TempestStateRestHandler
    @Inject constructor(val setttings: Settings,
                            restController: RestController,
                        val balancerState: BalancerState,
                        val client: Client) :
        BaseRestHandler(setttings, restController, client) {


    init {
        restController.registerHandler(RestRequest.Method.GET, "/_tempest", this)
    }

    override fun handleRequest(request: RestRequest, channel: RestChannel, client: Client) {
        val jsonBuilder = XContentFactory.jsonBuilder()

        jsonBuilder.startObject()
        jsonBuilder.field("lastOptimalBalanceFoundDateTime", balancerState.lastOptimalBalanceFoundDateTime)
        jsonBuilder.field("lastBalanceChangeDateTime", balancerState.lastBalanceChangeDateTime)
        jsonBuilder.field("lastRebalanceAttemptDateTime", balancerState.lastRebalanceAttemptDateTime)
        jsonBuilder.startArray("youngIndexes")
        balancerState.youngIndexes.forEach { jsonBuilder.value(it) }
        jsonBuilder.endArray()
        jsonBuilder.field("patternMap", balancerState.patternMapping as Map<String, RichIterable<String>>)
        jsonBuilder.field("clusterScore", balancerState.clusterScore)
        jsonBuilder.field("clusterRisk", balancerState.clusterRisk)
        jsonBuilder.field("balanceRatio", balancerState.clusterBalanceRatio)
        jsonBuilder.endObject()
        channel.sendResponse(BytesRestResponse(RestStatus.OK, jsonBuilder))
    }

}
