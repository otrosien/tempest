# Tempest
A pluggable balancer and shards allocator for Elasticsearch that balances a cluster based on shard sizes.

Elasticsearch's default allocator assigns and balances shards to nodes based on index-level and cluster-level settings. However, this approach can cause storage and memory issues when resources are limited or shared with other services. The tempest plugin replaces Elasticsearch's default allocator with one that allocates and balances shards based on the cumulative shard sizes on each node in the cluster. Balancing the cluster in this manner results in drastically increased stability and performance on homogeneous clusters.

# Relative Node Balancing
The allocator attempts to minimize the ratio of the sizes of the most full node to the least full node. That is, it attempts to distribute shards throughtout the cluster such that maxNode.size() / minNode.size() is minimized until it is below the ratio specified by cluster.routing.allocation.probabilistic.range_ratio, or 1.5 by default.

# Elasticsearch Version Support
Tempest is currently developed against Elasticsearch 1.4.2, but support has been tested up to Elasticsearch 1.7.1. Consequently, tempest can be used with an Elasticsearch cluster of any version in the range 1.4.2-1.7.1, and support may extend further. If there's a specific version you would like tested, please create an issue and we'll verify functionality.

# Build
If you prefer to build the plugin yourself rather than download from the releases page, you can do so very simply with maven. From project root directory:

    mvn clean package
    
Note that building yourself will append -SNAPSHOT to the the installable .zip file.

# Installation

Download the latest release from the releases page, then install it to your cluster using Elasticsearch's plugin script:

    elasticsearch-<es_version>/bin/plugin -url file:///path/to/tempest/target/releases/tempest-allocator-<version>.zip -install tempest

# Configuration (elasticsearch.yml)

    cluster.routing.allocation.type: com.datarank.tempest.allocator.LocalMinimumShardsAllocator
(Required) Replaces elasticsearch's default BalancedShardsAllocator behavior with tempest's LocalMinimumShardsAllocator behavior.

    cluster.routing.allocation.probabilistic.range_ratio : 1.5
(Optional) Modify the max_node_total_shard_size:min_node_total_shard_size goal ratio. Tempest will attempt to rebalance the cluster until the ratio is below the specified value. Defaults to 1.5.

    cluster.routing.allocation.probabilistic.iterations : 3000
(Optional) Modify the maximum number of random move operations that will be attempted during rebalance when searching for a better balanced cluster state. Defaults to (number of nodes * number of shards), where number of shards is primaries + replicas.
