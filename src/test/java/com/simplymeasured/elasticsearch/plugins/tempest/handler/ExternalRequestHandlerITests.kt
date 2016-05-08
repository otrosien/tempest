package com.simplymeasured.elasticsearch.plugins.tempest.handler

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs
import org.junit.Before
import org.junit.Test

/**
 * Created by awhite on 4/14/16.
 */
class ExternalRequestHandlerITests {
    private val runner = ElasticsearchClusterRunner()

    @Before
    @Throws(Exception::class)
    fun setUp() {
        runner.onBuild { index, settingsBuilder -> settingsBuilder.put("plugin.types", "com.simplymeasured.elasticsearch.plugins.tempest.TempestPlugin") }
              .build(newConfigs().numOfNode(1))
        runner.ensureYellow()
    }

    @Test
    @Throws(Exception::class)
    fun testTempestRestRequest() {
        while (true) {
        }
    }
}