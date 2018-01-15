# Tempest

## Overview

Tempest is a shard allocator and balancer for Elasticsearch. It is designed to balance by size rather than by shard
count. Additionally, Tempest tries to optimize primary shard placement to maximize bulk loading performance and 
OS page cache hit rates.    

The default balancer for Elasticsearch attempts to balance a cluster by equally distributing shards. For most use cases
this is sufficient; However, when custom routing is used, it is possible for some shards to have vastly different sizes.

## Features

The following are some of the high level features that Tempest offers (see below for details):

* Heuristic balancing
* Preemptive Balancing (in certain cases)
* Risk and overhead aware balancing
* Safety checks to prevent overly aggressive balancing
* REST endpoint for monitoring balance state and stats
* Highly Configurable

## Install

Current supported versions:

| ES Version   | Plugin URL                                                                                     |
| ------------ | -----------------------------------------------------------------------------------------------|
| 2.3.2        | https://github.com/datarank/tempest/releases/download/v2.1.0-ES2.3.2/tempest-2.1.0-ES2.3.2.zip
| 1.7.5        | https://github.com/datarank/tempest/releases/download/v2.0.2-ES1.7.5/tempest-2.0.2-ES1.7.5.zip
| 1.5.2        | https://github.com/datarank/tempest/releases/download/v2.0.2-ES1.5.2/tempest-2.0.2-ES1.5.2.zip
| 1.4.3        | https://github.com/datarank/tempest/releases/download/v2.0.2-ES1.4.3/tempest-2.0.2-ES1.4.3.zip

NOTE: Starting with ES 2.x, plugins will not load unless the plugin descriptor `elasticsearch.version` matches the
      server version *exactly*. Until we can find a way around this, it is very unlikely that we can have a tested
      release for every version of ES. However, in most cases all that is required is a repacking that can be done
      in-house. Simply fork this project, update `plugin-descriptor.properties`, run `mvn package`, and install the
      .zip locally.

## How it Works

Tempest uses a pseudo Monte-Carlo search algorithm coupled with a normalizing score function to find a series of moves
that should put the cluster in a more balanced state while minimizing risk and minimizing network overheard. The basic
process looks something like this:

1. Create an internal model of the current cluster state
2. Randomly generate several chains of moves (grouped in batches of size `cluster_concurrent_rebalance`)
3. Find the top N best chains that produce the best cluster state
4. Of the top N chains, select the one that minimizes risk and network overhead
5. Apply the first batch of the best chain

## Configuration

All setting are dynamic and are reloaded for each rebalance request. The defaults should be reasonable for clusters of
size 3 to 100 nodes.

### Searching

These settings control how Tempest will perform cluster move simulations:

* `tempest.balancer.searchDepth` - The number of move batches deep to search (default 5)
* `tempest.balancer.searchScaleFactor` - The number of move chains per search depth to consider (default 1000)
* `tempest.balancer.maxSearchTimeSeconds` - The maximum number of seconds to spend searching for a better state (default 5) 
* `tempest.balancer.searchQueueSize` - The number of best move chains to consider for final selection (default 10)
* `tempest.balancer.minimumShardMovementOverhead` - The cost associated with moving a small shard (default 100000000)
* `tempest.balancer.expungeBlacklistedNodes` - If true then shards on blacklisted nodes are moved to non-blacklisted nodes
 
The defaults for the settings are pretty good for most cases. Larger clusters (100+ nodes) might benefit from a shallower
search depth in favor of of a higher search scale factor.

### Limiting

These settings are used to prevent Tempest from being too aggressive or taking unnecessary risks when balancing.

* `tempest.balancer.maximumAllowedRiskRate` - Maximum allowed percent increase of the largest node during a chain (default 1.25)
* `tempest.balancer.minimumNodeSizeChangeRate` - Required relative score change of the most changed scoring group during a chain for the chain to be considered valid (default 0.25)

If you know you have a lot of capacity in your cluster, then increasing the allowed risk might help find some better states
when the cluster is already slightly balanced.

### Preemptive Balancing

These setting define how Tempest will model new indexes for preemptive balancing. Tempest does preemptive balancing
by grouping all indexes whose name matches a regex into a single group and then assuming that older indexes will act as
models for what newer indexes will look like. Then, when new indexes are created that match a pattern group,
their shards are allocated as if they were sized like the model indexes in that group.

* `tempest.balancer.groupingPatterns` - Comma separated list of regexes used for model grouping (default blank)
* `tempest.balancer.modelAgeMinutes` - Age in minutes at which an index is consider a model for preemptive balancing; Indexes newer than this are considered young and will be estimated using models (default 720)

Example:

Ths will create two groups, one for indexes like `twitter-` and one for indexes like `.marvel-`:

```
tempest.balancer.groupingPatterns: twitter-.*,\\.marvel-.*
```

Notes:

* Indexes that do not match any group are placed in a default group.

* If a group contains no model indexes then no shard size estimation is done.

* This feature can be disabled by setting the model age to 0 but be aware that this may cause many unassigned shards
  to be allocated on the most under utilized node.

## Balance Status

Tempest exposes a REST endpoint that is useful for verifying configuration and for seeing various stats. To access
the endpoint simply `GET` `http://hostname:9200/_tempest`


