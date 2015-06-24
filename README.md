# Tempest
A pluggable balancer and shards allocator for Elasticsearch that balances a cluster based on shard sizes.

Elasticsearch's default allocator assigns and balances shards to nodes based on index-level and cluster-level settings. However, this approach can cause storage and memory issues when resources are limited or shared. This plugin replaces the default allocator with one that allocates and balances shards based on the sizes of the shards.

# Relative Node Balancing
The allocator attempts to minimize the ratio of the sizes of the largest node in your cluster to the smallest node in your cluster. That is, it attempts to reorganize shards on nodes such that maxNode.size() / minNode.size() is minimized until it is below the ratio specified by cluster.routing.allocation.probabilistic.range_ratio, or 1.5 by default.

# Build
From project root directory:

    mvn clean package

# Installation

    elasticsearch-<version>/bin/plugin -url file:///path/to/tempest/target/releases/tempest-allocator-<version>-SNAPSHOT.zip -install tempest

# Configuration (elasticsearch.yml)

    cluster.routing.allocation.type: com.datarank.tempest.allocator.LocalMinimumShardsAllocator
(Required) Replaces elasticsearch's default BalancedShardsAllocator behavior with tempest's LocalMinimumShardsAllocator behavior.

    cluster.routing.allocation.probabilistic.range_ratio : 1.5
(Optional) Modify the max_node_total_shard_size:min_node_total_shard_size goal ratio. Tempest will attempt to rebalance the cluster until the ratio is below the specified value. Defaults to 1.5.

    cluster.routing.allocation.probabilistic.iterations : 3000
(Optional) Modify the maximum number of random move operations that will be attempted during rebalance when searching for a better balanced cluster state. Defaults to number of nodes * number of shards, where number of shards is primaries + replicas.
