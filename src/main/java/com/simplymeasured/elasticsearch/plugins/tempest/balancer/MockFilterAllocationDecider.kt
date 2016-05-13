package com.simplymeasured.elasticsearch.plugins.tempest.balancer

import org.elasticsearch.cluster.node.DiscoveryNodeFilters
import org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.AND
import org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.OR
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider.*
import org.elasticsearch.common.settings.Settings

/**
 * Partial implementation of org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider
 *
 * Note: Index awareness is not not implemented. Node awareness should be fully supported.
 */
class MockFilterAllocationDecider(settings: Settings) : MockDecider {
    private val clusterRequireFilters: DiscoveryNodeFilters?
    private val clusterIncludeFilters: DiscoveryNodeFilters?
    private val clusterExcludeFilters: DiscoveryNodeFilters?

    init {
        clusterRequireFilters = settings.getByPrefix(CLUSTER_ROUTING_REQUIRE_GROUP).getAsMap().let { if (it.isEmpty()) null else DiscoveryNodeFilters.buildFromKeyValue(AND, it) }
        clusterIncludeFilters = settings.getByPrefix(CLUSTER_ROUTING_INCLUDE_GROUP).getAsMap().let { if (it.isEmpty()) null else DiscoveryNodeFilters.buildFromKeyValue(OR, it) }
        clusterExcludeFilters = settings.getByPrefix(CLUSTER_ROUTING_EXCLUDE_GROUP).getAsMap().let { if (it.isEmpty()) null else DiscoveryNodeFilters.buildFromKeyValue(OR, it) }
    }

    override fun canMove(shard: ModelShard, destNode: ModelNode, moves: Collection<MoveAction>): Boolean {
        if (clusterExcludeFilters?.match(destNode.backingNode.node())   ?: false) { return false }
        if (!(clusterIncludeFilters?.match(destNode.backingNode.node()) ?: true)) { return false }
        if (!(clusterRequireFilters?.match(destNode.backingNode.node()) ?: true)) { return false }
        return true
    }
}