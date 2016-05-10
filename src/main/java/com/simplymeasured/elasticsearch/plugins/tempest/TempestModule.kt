package com.simplymeasured.elasticsearch.plugins.tempest

import com.simplymeasured.elasticsearch.plugins.tempest.handler.TempestStateRestHandler
import org.elasticsearch.cluster.ClusterModule
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocators
import org.elasticsearch.common.inject.AbstractModule
import org.elasticsearch.common.inject.Inject

class TempestModule()  : AbstractModule() {

    override fun configure() {
        bind(TempestStateRestHandler::class.java).asEagerSingleton()
        bind(BalancerState::class.java).asEagerSingleton()
    }
}