package com.simplymeasured.elasticsearch.plugins.tempest.handlers

import com.simplymeasured.elasticsearch.plugins.tempest.TempestShardsAllocator
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.ModelCluster
import org.eclipse.collections.api.RichIterable
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.routing.RoutingService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.inject.Provider
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.rest.*
import org.joda.time.DateTime

/**
 * Simple reporting REST handler that exposes balancer's stats on /_tempest
 */
class TempestRebalanceRestHandler
@Inject constructor(settings: Settings,
                    restController: RestController,
                    client: Client) :
        BaseRestHandler(settings, restController, client) {

    init {
        restController.registerHandler(RestRequest.Method.POST, "/_tempest/rebalance", this)
    }

    override fun handleRequest(request: RestRequest, channel: RestChannel, client: Client) {
        client.admin().cluster().prepareReroute().execute()
        channel.sendResponse(BytesRestResponse(RestStatus.OK))
    }
}