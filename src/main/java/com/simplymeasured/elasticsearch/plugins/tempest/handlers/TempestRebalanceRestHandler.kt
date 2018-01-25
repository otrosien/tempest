package com.simplymeasured.elasticsearch.plugins.tempest.handlers

import org.elasticsearch.client.Client
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.rest.*

/**
 * Simple reporting REST handler that exposes balancer's stats on /_tempest
 */
class TempestRebalanceRestHandler
@Inject constructor(settings: Settings,
                    restController: RestController) :
        BaseRestHandler(settings) {

    init {
        restController.registerHandler(RestRequest.Method.POST, "/_tempest/rebalance", this)
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        client.admin().cluster().prepareReroute().execute()

        return RestChannelConsumer { channel -> channel.sendResponse(BytesRestResponse(RestStatus.OK, "Submitted")) }
    }

}