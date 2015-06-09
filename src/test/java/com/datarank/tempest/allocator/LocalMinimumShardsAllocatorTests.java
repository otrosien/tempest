package com.datarank.tempest.allocator;

import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.GatewayAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocatorModule;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocators;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Locale;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class LocalMinimumShardsAllocatorTests extends ElasticsearchAllocationTestCase {
    private final ESLogger logger = Loggers.getLogger(LocalMinimumShardsAllocatorTests.class);
    private int numberOfNodes;
    private int numberOfIndices;
    private int[] numberOfShards;
    private int numberOfReplicas;
    private int totalShards;
    private Settings settings;

    private ClusterInfo clusterInfo;

    @Mock
    private ClusterInfoService clusterInfoService;

    @Test
    public void testSizeBalance() {
        clusterInfoService = Mockito.mock(InternalClusterInfoService.class);
        initRandom();
        buildRandomlyDistributedClusterInfo();
        Mockito.when(clusterInfoService.getClusterInfo()).thenReturn(clusterInfo);
        clusterBalances(new MaxMinBalanceAssertion());
    }

    private void clusterBalances(final MaxMinBalanceAssertion assertion) {
        final double maxRatio = 1.5;

        settings = settingsBuilder()
                .put(ShardsAllocatorModule.TYPE_KEY, LocalMinimumShardsAllocator.class.getCanonicalName())
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString())
                .put(LocalMinimumShardsAllocator.SETTING_MAX_MIN_RATIO_THRESHOLD, maxRatio)
                .build();

        AllocationService strategy = new AllocationService(settings, randomAllocationDeciders(settings,
                new NodeSettingsService(settings), getRandom()),
                new ShardsAllocators(settings, new NoopGatewayAllocator(), new LocalMinimumShardsAllocator(settings)), clusterInfoService);

        ClusterState clusterstate = initCluster(strategy);
        assertion.assertBalanced(clusterstate.getRoutingNodes(), maxRatio);

        clusterstate = addNode(clusterstate, strategy);
        assertion.assertBalanced(clusterstate.getRoutingNodes(), maxRatio);

        clusterstate = removeNodes(clusterstate, strategy);
        assertion.assertBalanced(clusterstate.getRoutingNodes(), maxRatio);
    }

    private void initRandom() {
        numberOfNodes = between(3, 100);
        numberOfIndices = between(1, 10);
        numberOfReplicas = 1;
        totalShards = 0;
        numberOfShards = new int[numberOfIndices];
        for (int i = 0; i < numberOfIndices; i++) {
            numberOfShards[i] = getRandom().nextInt(2)*numberOfNodes;
            totalShards += numberOfShards[i];
        }
    }

    private ClusterState initCluster(final AllocationService strategy) {
        MetaData.Builder metaDataBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        for (int i = 0; i < numberOfIndices; i++) {
            IndexMetaData.Builder index = IndexMetaData.builder("test" + i).settings(settings).numberOfShards(numberOfShards[i]).numberOfReplicas(numberOfReplicas);
            metaDataBuilder = metaDataBuilder.put(index);
        }

        MetaData metaData = metaDataBuilder.build();

        for (ObjectCursor<IndexMetaData> cursor : metaData.indices().values()) {
            routingTableBuilder.addAsNew(cursor.value);
        }

        RoutingTable routingTable = routingTableBuilder.build();

        logger.info("start {} nodes for {} indexes with {} total shards and {} replicas", numberOfNodes, numberOfIndices, totalShards, numberOfReplicas);
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfNodes; i++) {
            nodes.put(newNode("node" + i));
        }
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).nodes(nodes).metaData(metaData).routingTable(routingTable).build();
        routingTable = strategy.reroute(clusterState).routingTable(); // starts shards
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.routingNodes();
        clusterState = rebalance(strategy, routingTable, routingNodes, clusterState);

        return clusterState;
    }

    private ClusterState addNode(ClusterState clusterState, final AllocationService strategy) {
        logger.info("now, start 1 more node, check that rebalancing will happen because we set it to always");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(newNode("node" + numberOfNodes)))
                .build();

        RoutingTable routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        // move initializing to started
        clusterState = rebalance(strategy, routingTable, routingNodes, clusterState);

        numberOfNodes++;
        return clusterState;
    }

    private ClusterState removeNodes(ClusterState clusterState, final AllocationService strategy) {
        logger.info("Removing half the nodes (" + (numberOfNodes + 1) / 2 + ")");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes());

        for (int i = numberOfNodes / 2; i <= numberOfNodes; i++) {
            nodes.remove("node" + i);
        }
        numberOfNodes = numberOfNodes / 2;

        clusterState = ClusterState.builder(clusterState).nodes(nodes.build()).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        logger.info("start all the primary shards, replicas will start initializing");
        RoutingTable routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("start the replica shards");
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("rebalancing");
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("complete rebalancing");
        clusterState = rebalance(strategy, routingTable, routingNodes, clusterState);

        return clusterState;
    }

    private ClusterState rebalance(final AllocationService strategy, RoutingTable routingTable, RoutingNodes routingNodes, ClusterState clusterState) {
        RoutingTable prev = routingTable;
        int steps = 0;
        while (true) {
            routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
            routingNodes = clusterState.routingNodes();
            if (routingTable == prev && allActive(routingTable)) {
                logger.info("done after {} steps", steps);
                return clusterState;
            }
            prev = routingTable;
            if (steps != 0 && steps % 10 == 0) {
                logger.info("completed {} steps", steps);
            }
            steps++;
        }
    }

    private boolean allActive(final RoutingTable routingTable) {
        for (IndexRoutingTable index: routingTable) {
            for (IndexShardRoutingTable shard: index) {
                for (ShardRouting replica: shard) {
                    if (!replica.started()) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private void buildRandomlyDistributedClusterInfo() {
        long[] sizes = new long[numberOfIndices];
        for (int i = 0; i < numberOfIndices; i++) {
            // Pick a random size with probabilities similar to those on the
            // Wikimedia cluster (just because I know the probabilites, not
            // because it is special)
            double size;
            if (numberOfShards[i] == 1) {
                // Single shard indices are normally small (10s of MB) or
                // hovering around 2GB.
                if (rarely()) {
                    size = (1 + getRandom().nextInt(30)) * 102.4 * 1024 * 1024;
                } else {
                    size = getRandom().nextDouble() * 10 * 1024 * 1024;
                }
            } else {
                // Multi shard indexes are sometimes huge (around 20GB) but normally
                // about the size of a large of a large single shard index.
                if (rarely()) {
                    // Rarely tens of gigabyte sized
                    size = (long)(1 + getRandom().nextInt(30)) * 1024 * 1024 * 1024;
                } else {
                    // Reasonably commonly in the 100MB to 2GB range
                    size = (1 + getRandom().nextInt(30)) * 102.4 * 1024 * 1024;
                }
            }
            sizes[i] = (long)size;
        }
        buildClusterInfo(sizes);
    }

    private void buildEvenlyDistributedClusterInfo() {
        long[] sizes = new long[numberOfIndices];
        // Set the sizes far enough appart that they don't end up in the same cohort.
        double nextSize = Math.log10(1024 * 1024 * 1024);
        for (int i = 0; i < numberOfIndices; i++) {
            sizes[i] = (long)Math.pow(10, nextSize);
            nextSize *= 10;
        }
        buildClusterInfo(sizes);
    }

    private void buildZeroSizedClusterInfo() {
        long[] sizes = new long[numberOfIndices];
        Arrays.fill(sizes, 0);
        buildClusterInfo(sizes);
    }

    private void buildClusterInfo(final long[] sizes) {
        ImmutableMap.Builder<String, Long> shardSizes = ImmutableMap.builder();
        for (int i = 0; i < numberOfIndices; i++) {
            for (int s = 0; s < numberOfShards[i]; s++) {
                String sid = String.format(Locale.US, "[test%s][%s]", i, s);
                shardSizes.put(sid + "[p]", sizes[i]);
                shardSizes.put(sid + "[r]", sizes[i]);
            }
        }
        clusterInfo = new ClusterInfo(ImmutableMap.<String, DiskUsage> of(), shardSizes.build());
    }

    private class MaxMinBalanceAssertion {
        public void assertBalanced(final RoutingNodes nodes, final double maxRatio) {
            assertFalse(nodes.hasUnassignedShards());
            long maxSize = Long.MIN_VALUE;
            long minSize = Long.MAX_VALUE;
            long nodeSize;
            for (RoutingNode node : nodes) {
                nodeSize = 0l;
                for (MutableShardRouting shard : node) {
                    nodeSize += clusterInfo.getShardSizes().get(InternalClusterInfoService.shardIdentifierFromRouting(shard));
                }
                if (nodeSize > maxSize) {
                    maxSize = nodeSize;
                }
                if (nodeSize < minSize) {
                    minSize = nodeSize;
                }
            }
            assertThat((double)maxSize / (double)minSize, lessThanOrEqualTo(maxRatio));
        }
    }

    /**
     * An allocator used for tests that doesn't do anything
     */
    class NoopGatewayAllocator implements GatewayAllocator {

        @Override
        public void applyStartedShards(StartedRerouteAllocation allocation) {
            // noop
        }

        @Override
        public void applyFailedShards(FailedRerouteAllocation allocation) {
            // noop
        }

        @Override
        public boolean allocateUnassigned(RoutingAllocation allocation) {
            return false;
        }
    }
}
