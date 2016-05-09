package com.simplymeasured.elasticsearch.plugins.tempest

import com.simplymeasured.elasticsearch.plugins.tempest.balancer.ModelCluster
import org.eclipse.collections.impl.factory.Lists

/**
 * Created by awhite on 5/9/16.
 */
class ModelClusterTracker {
    val modelClusters = Lists.mutable.empty<ModelCluster>()

    fun add(modelCluster: ModelCluster) {
        modelClusters.add(modelCluster)
    }

    override fun toString(): String {
        val stringBuilder = StringBuilder()
        modelClusters.forEachIndexed { index, modelCluster ->
            stringBuilder
                    .append(index)
                    .append(",")
                    .append(modelCluster.modelNodes.map { it.calculateUsage() }.joinToString())
                    .append("\n")
        }

        return stringBuilder.toString()
    }
}