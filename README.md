# Tempest
A pluggable balancer and shards allocator for Elasticsearch that balances a cluster based on shard sizes.

Elasticsearch's default allocator assigns and balances shards to nodes based on index-level and cluster-level settings. However, this approach can cause storage and memory issues when resources are limited or shared. This plugin replaces the default allocator with one that allocates and balances shards based on the sizes of the shards.

# Relative Node Balancing
The allocator attempts to minimize the ratio of the sizes of the largest node in your cluster to the smallest node in your cluster. That is, it attempts to reorganize shards on nodes such that maxNode.size() / minNode.size() is minimized until it is below the ratio specified by cluster.routing.allocation.probabilistic.range_ratio, or 1.5 by default.

# Installation
TODO

#Usage
TODO
