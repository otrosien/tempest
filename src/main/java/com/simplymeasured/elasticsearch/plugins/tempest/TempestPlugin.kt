package com.simplymeasured.elasticsearch.plugins.tempest

import com.simplymeasured.elasticsearch.plugins.tempest.balancer.HeuristicBalancer
import org.elasticsearch.cluster.ClusterModule
import org.elasticsearch.common.inject.Module
import org.elasticsearch.plugins.Plugin

/**
 * Created by awhite on 4/14/16.
 */

class TempestPlugin : Plugin() {
    override fun name() = "tempest"

    override fun description() = "shard balancer"

    override fun nodeModules() = mutableListOf(TempestModule())

    fun onModule(clusterModule: ClusterModule) {
        clusterModule.registerShardsAllocator("tempest", TempestShardsAllocator::class.java)
    }
}

