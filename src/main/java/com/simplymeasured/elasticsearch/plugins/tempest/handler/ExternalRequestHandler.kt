package com.simplymeasured.elasticsearch.plugins.tempest.handler

import org.elasticsearch.common.inject.Inject
import org.elasticsearch.rest.*

/**
 * Created by awhite on 4/14/16.
 */
class ExternalRequestHandler @Inject constructor(restController: RestController) : RestHandler {

    init {
        restController.registerHandler(RestRequest.Method.GET, "/_tempest", this)
    }

    override fun handleRequest(restRequest: RestRequest, restChannel: RestChannel) {
        restChannel.sendResponse(BytesRestResponse(RestStatus.OK, "Hello World"))
    }
}
