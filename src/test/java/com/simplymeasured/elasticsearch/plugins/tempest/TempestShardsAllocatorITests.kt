package com.simplymeasured.elasticsearch.plugins.tempest

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner
import org.elasticsearch.common.settings.Settings
import org.junit.After
import org.junit.Before
import org.junit.Test

/**
 * Created by awhite on 4/15/16.
 */
class TempestShardsAllocatorITests {
    private val runner = ElasticsearchClusterRunner()

    @Before
    @Throws(Exception::class)
    fun setUp() {
        runner.onBuild { index, settingsBuilder ->
            settingsBuilder.put("logger.com.simplymeasured.elasticsearch.plugins.tempest", "TRACE")
            settingsBuilder.put("tempest.balancer.groupingPatterns", "index-\\w+,index-\\w+-\\d+")
            
            settingsBuilder.put("plugin.types", "com.simplymeasured.elasticsearch.plugins.tempest.TempestPlugin")
            settingsBuilder.put("cluster.routing.allocation.type", "tempest")
            settingsBuilder.put("cluster.routing.allocation.same_shard.host", false)
            settingsBuilder.put("http.cors.enabled", true);
            settingsBuilder.put("http.cors.allow-origin", "*");
            settingsBuilder.putArray("discovery.zen.ping.unicast.hosts", "localhost:9301-9305");
        }.build(ElasticsearchClusterRunner.newConfigs().numOfNode(5))

        runner.ensureGreen()
    }

    @After
    fun tearDown() {
        runner.close()
        runner.clean()
    }

    @Test
    @Throws(Exception::class)
    fun testTempestShardsAllocator() {
        runner.createIndex("index-a", Settings.settingsBuilder().put("index.number_of_replicas", "2").build())
        runner.createIndex("index-b", Settings.settingsBuilder().put("index.number_of_replicas", "2").build())
        runner.createIndex("index-c-123", Settings.settingsBuilder().put("index.number_of_replicas", "2").build())

        while (true) {
        }
    }
}