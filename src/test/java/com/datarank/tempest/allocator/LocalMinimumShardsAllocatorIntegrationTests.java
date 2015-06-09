/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datarank.tempest.allocator;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocatorModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.instanceOf;

public class LocalMinimumShardsAllocatorIntegrationTests extends ElasticsearchIntegrationTest {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(ShardsAllocatorModule.TYPE_KEY, "org.elasticsearch.cluster.routing.allocation.allocator.probabilistic.LocalMinimumShardsAllocator")
                .build();
    }

    @Test
    public void testRandom() {
        client().admin().indices().prepareCreate("index_0").execute().actionGet();
        ensureGreen("index_0");
        for (int i = 0; i < 5000; i++) {
            indexRandom(0);
        }
        client().admin().indices().prepareCreate("index_1").execute().actionGet();
        ensureGreen("index_1");
        for (int i = 0; i < 1000; i++) {
            indexRandom(0);
        }
        ensureGreen("index_0", "index_1");
    }

    @Test
    public void testAllocator() throws IOException, InterruptedException{
        internalCluster().ensureAtLeastNumDataNodes(3);
        internalCluster().ensureAtMostNumDataNodes(3);
        client().admin().indices().prepareCreate("index_0")
                .setSettings(settingsBuilder()
                        .put("number_of_shards", 1)
                        .put("number_of_replicas", 1))
                .execute().actionGet();


        ensureGreen("index_0");
        refresh();

        for (int i = 0; i < 5000; i++) {
            indexRandom(0);
        }
        refresh();

        client().admin().indices().prepareCreate("index_1")
                .setSettings(settingsBuilder()
                        .put("number_of_shards", 1)
                        .put("number_of_replicas", 1))
                .execute().actionGet();

        refresh();

        for (int i = 0; i < 1000; i++) {
            indexRandom(1);
        }

        refresh();
        ensureGreen("index_0", "index_1");
    }

    @After
    public void checkStatus() {
        refresh();
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        RoutingNodes routingNodes = state.routingNodes();
    }

    private IndexResponse indexRandom(int indexNumber) {
        IndexRequest indexRequest = new IndexRequest("index_" + indexNumber, "type", RandomizedTest.randomAsciiOfLength(25));
        indexRequest.source("field", RandomizedTest.randomAsciiOfLengthBetween(10, 1000));
        IndexResponse response = client().index(indexRequest).actionGet();

        return response;
    }

    private BulkResponse buildIndexBulk(int indexNumber, int numDocs) {
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(Requests.indexRequest("test_" + indexNumber).type("type").source("field", RandomizedTest.randomAsciiOfLengthBetween(10, 1000)));
        }
        return client().bulk(bulkRequest.request()).actionGet();
    }

    private void assertAllocatorInstance(Settings settings, Class<? extends ShardsAllocator> clazz) throws IOException {
        while (cluster().size() != 0) {
            internalCluster().stopRandomDataNode();
        }
        internalCluster().startNode(settings);
        ShardsAllocator instance = internalCluster().getInstance(ShardsAllocator.class);
        assertThat(instance, instanceOf(clazz));
    }
}
