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

package com.simplymeasured.elasticsearch.plugins.tempest.handlers

import com.simplymeasured.elasticsearch.plugins.tempest.actions.TempestInfoAction
import com.simplymeasured.elasticsearch.plugins.tempest.actions.TempestInfoRequest
import com.simplymeasured.elasticsearch.plugins.tempest.actions.TempestInfoResponse
import org.elasticsearch.client.Client
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.rest.*
import org.elasticsearch.rest.action.RestStatusToXContentListener

/**
 * Created by awhite on 2/5/17.
 */

class TempestInfoRestHandler
@Inject constructor(settings: Settings,
                    restController: RestController) :
        BaseRestHandler(settings) {

    init {
        restController.registerHandler(RestRequest.Method.GET, "/_tempest", this)
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        return RestChannelConsumer{ channel ->
            client.execute(
                TempestInfoAction(),
                TempestInfoRequest(),
                RestStatusToXContentListener<TempestInfoResponse>(channel)) }
    }
}