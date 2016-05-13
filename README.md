# Tempest

## Overview

Tempest is a shard allocator and balancer for Elasticsearch. It is designed to balance by size rather than by shard
count.

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

TBD

## Configuration

All setting are dynamic are are reloaded for each rebalance request. The defaults should be reasonable for clusters of
size 3 to 50 nodes.

### Searching

These settings control how Tempest will perform cluster move simulations:

* `tempest.balancer.searchDepth` - The number of move batches deep to search (default 8)
* `tempest.balancer.searchScaleFactor` - The number of move chains per search depth to consider (default 1000)
* `tempest.balancer.searchQueueSize` - The number of best move chains to consider for final selection (default 10)
* `tempest.balancer.minimumShardMovementOverhead` - The cost associated with moving a small shard (default 100000000)

The defaults for the settings are pretty good for most cases. Larger clusters (100+ nodes) might benefit from a shallower
search depth in favor of of a higher search scale factor.

### Limiting

These settings are used to prevent Tempest from being too aggressive with balancing. In particular, they should help prevent
odd intermediate states that put strain on a single node and prevent costly rebalances that minimum impact on the overall
balance.

* `tempest.balancer.maximumAllowedRiskRate` - Maximum allowed percent increase of the largest node during a chain (default 1.10)
* `tempest.balancer.forceRebalanceThresholdMinutes` - Maximum allowed time before a rebalance is forced (default 60); Note, does not actually schedule rebalances
* `tempest.balancer.minimumNodeSizeChangeRate` - Required size change of the most changed node during a chain for the chain to be considered valid (default 0.10)

If you know you have a lot of capacity in your cluster, then increasing the allowed risk might help find some better states
when the cluster is already slightly balanced.

The `forceRebalanceThresholdMinutes` is there to prevent excessive rebalancing in the event that Elasticsearch calls the
rebalance too aggressively. The setting does not schedule rebalances in any way.

In clusters with lots of very small shards, it may be necessary to reduce `minimumNodeSizeChangeRate` in order to prevent
overly aggressive balancing.

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

Indexes that do not match any group are placed in a default group.

If a group contains no model indexes then no shard size estimation is done.

This feature can be disabled by setting the model age to 0

## Balance Status

Tempest exposes a REST endpoint that is useful for verifying configuration and for seeing various stats. To access
the endpoint simply `GET` `http://hostname:9200/_tempest`


