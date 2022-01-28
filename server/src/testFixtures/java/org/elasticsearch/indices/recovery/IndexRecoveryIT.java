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

package org.elasticsearch.indices.recovery;

import static io.crate.testing.TestingHelpers.printedTable;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.node.RecoverySettingsChunkSizePlugin.CHUNK_SIZE_SETTING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.recovery.RecoveryState.Stage;
import org.elasticsearch.node.RecoverySettingsChunkSizePlugin;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.junit.Ignore;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.integrationtests.SQLIntegrationTestCase;
import io.crate.metadata.IndexParts;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndexRecoveryIT extends SQLIntegrationTestCase {

    private static final String INDEX_NAME = "test_idx_1";
    private static final String INDEX_TYPE = "test-type-1";
    private static final String REPO_NAME = "test_repo_1";
    private static final String SNAP_NAME = "test_snap_1";

    private static final int MIN_DOC_COUNT = 500;
    private static final int MAX_DOC_COUNT = 1000;
    private static final int SHARD_COUNT = 1;
    private static final int REPLICA_COUNT = 0;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.addAll(super.nodePlugins());
        plugins.addAll(Arrays.asList(
            MockTransportService.TestPlugin.class,
            MockFSIndexStore.TestPlugin.class,
            RecoverySettingsChunkSizePlugin.class,
            TestAnalysisPlugin.class,
            InternalSettingsPlugin.class,
            MockEngineFactoryPlugin.class));
        return plugins;
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        super.beforeIndexDeletion();
        internalCluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
        internalCluster().assertSeqNos();
        internalCluster().assertSameDocIdsOnShards();
    }

    private void assertRecoveryStateWithoutStage(RecoveryState state, int shardId, RecoverySource.Type recoverySource, boolean primary,
                                                 String sourceNode, String targetNode) {
        assertThat(state.shardId(), equalTo(shardId));
        assertThat(state.recoverySource(), equalTo(recoverySource));
        assertThat(state.primary(), equalTo(primary));
        if (sourceNode == null) {
            assertNull(state.sourceNode());
        } else {
            assertNotNull(state.sourceNode());
            assertThat(state.sourceNode(), equalTo(sourceNode));
        }
        if (targetNode == null) {
            assertNull(state.targetNode());
        } else {
            assertNotNull(state.targetNode());
            assertThat(state.targetNode(), equalTo(targetNode));
        }
    }

    private void assertRecoveryState(RecoveryState state, int shardId, RecoverySource.Type type, boolean primary, Stage stage,
                                     String sourceNode, String targetNode) {
        assertRecoveryStateWithoutStage(state, shardId, type, primary, sourceNode, targetNode);
        assertThat(state.stage(), equalTo(stage));
    }

    private void assertOnGoingRecoveryState(RecoveryState state, int shardId, RecoverySource.Type type, boolean primary,
                                            String sourceNode, String targetNode) {
        assertRecoveryStateWithoutStage(state, shardId, type, primary, sourceNode, targetNode);
        assertThat(state.stage(), not(equalTo(Stage.DONE)));
    }

    private void slowDownRecovery(long shardSizeBytes) {
        long chunkSize = Math.max(1, shardSizeBytes / 10);
        assertTrue(client().admin().cluster().prepareUpdateSettings()
                       .setTransientSettings(Settings.builder()
                                                 // one chunk per sec..
                                                 .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), chunkSize, ByteSizeUnit.BYTES)
                                                 // small chunks
                                                 .put(CHUNK_SIZE_SETTING.getKey(), new ByteSizeValue(chunkSize, ByteSizeUnit.BYTES))
                       ).get().isAcknowledged());
    }

    private void restoreRecoverySpeed() {
        assertTrue(client().admin().cluster().prepareUpdateSettings()
                       .setTransientSettings(Settings.builder()
                                                 .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "20mb")
                                                 .put(CHUNK_SIZE_SETTING.getKey(), RecoverySettings.DEFAULT_CHUNK_SIZE)
                       ).get().isAcknowledged());
    }

    private static record RecoveryState(int shardId,
                                         boolean primary,
                                         Stage stage,
                                         RecoverySource.Type recoverySource,
                                         long time,
                                         float recoveredFilesPercent,
                                         float recoveredBytesPercent,
                                         String sourceNode,
                                         String targetNode) {
    }

    @Nullable
    private RecoveryState recoveryState(String node, String indexName) {
        var response  = execute("SELECT " +
                " id," +
                " primary," +
                " recovery['stage']," +
                " recovery['type']," +
                " recovery['total_time']," +
                " recovery['files']['percent']," +
                " recovery['size']['percent']," +
                " recovery['source_node']['name']," +
                " recovery['target_node']['name']" +
                " FROM sys.shards WHERE table_name = '" + indexName + "' AND node['name'] = '" + node + "'");
        System.out.println("shard response on node '" + node + ": " + printedTable(response.rows()));
        if (response.rowCount() == 0L) {
            return null;
        }
        var res = response.rows()[0];
        return new RecoveryState(
            (int) res[0],
            (boolean) res[1],
            Stage.valueOf((String) res[2]),
            RecoverySource.Type.valueOf((String) res[3]),
            (long) res[4],
            (float) res[5],
            (float) res[6],
            (String) res[7],
            (String) res[8]
        );
    }

    private void validateIndexRecoveryState(RecoveryState recoveryState) {
        assertThat(recoveryState.time(), greaterThanOrEqualTo(0L));
        assertThat(recoveryState.recoveredFilesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(recoveryState.recoveredFilesPercent(), lessThanOrEqualTo(100.0f));
        assertThat(recoveryState.recoveredBytesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(recoveryState.recoveredBytesPercent(), lessThanOrEqualTo(100.0f));
    }

    private void createAndPopulateIndex(String name, int nodeCount, int shardCount, int replicaCount)
        throws ExecutionException, InterruptedException {

        logger.info("--> creating test index: {}", name);
        execute("CREATE TABLE " + name + " (foo_int INT, foo_string TEXT, foo_float FLOAT) " +
                " CLUSTERED INTO " + shardCount + " SHARDS WITH (number_of_replicas=" + replicaCount + "," +
                " \"store.stats_refresh_interval\"=0)");
        //assertAcked(prepareCreate(name, nodeCount, Settings.builder().put("number_of_shards", shardCount)
        //    .put("number_of_replicas", replicaCount).put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), 0)));
        ensureGreen();

        logger.info("--> indexing sample data");
        final int numDocs = between(MIN_DOC_COUNT, MAX_DOC_COUNT);
        final Object[][] docs = new Object[numDocs][];

        for (int i = 0; i < numDocs; i++) {
            docs[i] = new Object[]{
                randomInt(),
                randomAlphaOfLength(32),
                randomFloat()
            };
        }

        execute("INSERT INTO " + name + " (foo_int, foo_string, foo_float) VALUES (?, ?, ?)", docs);
        execute("REFRESH TABLE " + name);
        execute("OPTIMIZE TABLE " + name);
        execute("SELECT COUNT(*) FROM " + name);
        assertThat(response.rows()[0][0], is((long) numDocs));
        //assertThat(client().prepareSearch(name).setSize(0).get().getHits().getTotalHits().value, equalTo((long) numDocs));
        //return client().admin().indices().prepareStats(name).execute().actionGet(5, TimeUnit.SECONDS);
    }

    @Test
    public void testGatewayRecovery() throws Exception {
        logger.info("--> start nodes");
        String node = internalCluster().startNode();

        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> restarting cluster");
        internalCluster().fullRestart();
        ensureGreen();

        logger.info("--> request recoveries");
        var recoveryState = recoveryState(node, INDEX_NAME);
        assertThat(recoveryState.stage(), is(Stage.DONE));
        assertThat(recoveryState.recoverySource(), is(RecoverySource.Type.EXISTING_STORE));

        validateIndexRecoveryState(recoveryState);
    }

    @Test
    public void testGatewayRecoveryTestActiveOnly() throws Exception {
        logger.info("--> start nodes");
        internalCluster().startNode();

        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> restarting cluster");
        internalCluster().fullRestart();
        ensureGreen();

        logger.info("--> request recoveries");
        execute("SELECT * FROM sys.shards WHERE table_name = '" + INDEX_NAME + "'" +
                " AND recovery['stage'] != 'DONE'");
        assertThat(response.rowCount(), is(0L));  // Should not expect any responses back
    }

    @Test
    public void testReplicaRecovery() throws Exception {
        final String nodeA = internalCluster().startNode();

        execute("CREATE TABLE " + INDEX_NAME + " (id BIGINT, data TEXT) " +
                " CLUSTERED INTO " + SHARD_COUNT + " SHARDS WITH (number_of_replicas=" + REPLICA_COUNT + ")");
        ensureGreen();

        final int numOfDocs = scaledRandomIntBetween(0, 200);
        try (BackgroundIndexer indexer = new BackgroundIndexer(
            IndexParts.toIndexName(sqlExecutor.getCurrentSchema(), INDEX_NAME, null),
            "data",
            sqlExecutor.jdbcUrl(),
            numOfDocs,
            scaledRandomIntBetween(2, 5),
            true,
            null)) {
            waitForDocs(numOfDocs, indexer, sqlExecutor);
        }

        refresh();
        execute("SELECT COUNT(*) FROM " + INDEX_NAME);
        assertThat(response.rows()[0][0], is((long) numOfDocs));

        // We do not support ALTER on a closed table
        //final boolean closedIndex = randomBoolean();
        final boolean closedIndex = false;
        if (closedIndex) {
            execute("ALTER TABLE " + INDEX_NAME + " CLOSE");
            ensureGreen();
        }

        // force a shard recovery from nodeA to nodeB
        final String nodeB = internalCluster().startNode();
        execute("ALTER TABLE " + INDEX_NAME + " SET (number_of_replicas=1)");
        ensureGreen();


        // we should now have two total shards, one primary and one replica
        execute("SELECT * FROM sys.shards WHERE table_name = '" + INDEX_NAME + "'");
        assertThat(response.rowCount(), is(2L));

        // validate node A recovery
        final RecoveryState nodeARecoveryState = recoveryState(nodeA, INDEX_NAME);
        final RecoverySource.Type expectedRecoverySourceType;
        if (closedIndex == false) {
            expectedRecoverySourceType = RecoverySource.Type.EMPTY_STORE;
        } else {
            expectedRecoverySourceType = RecoverySource.Type.EXISTING_STORE;
        }
        assertRecoveryState(nodeARecoveryState, 0, expectedRecoverySourceType, true, Stage.DONE, null, nodeA);
        validateIndexRecoveryState(nodeARecoveryState);

        // validate node B recovery
        final RecoveryState nodeBRecoveryState = recoveryState(nodeB, INDEX_NAME);
        assertRecoveryState(nodeBRecoveryState, 0, RecoverySource.Type.PEER, false, Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryState);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeA));

        if (closedIndex) {
            execute("ALTER TABLE " + INDEX_NAME + " OPEN");
        }
        execute("SELECT COUNT(*) FROM " + INDEX_NAME);
        assertThat(response.rows()[0][0], is((long) numOfDocs));
    }

    @Test
    public void testCancelNewShardRecoveryAndUsesExistingShardCopy() throws Exception {
        logger.info("--> start node A");
        final String nodeA = internalCluster().startNode();

        logger.info("--> create index on node: {}", nodeA);
        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> start node B");
        // force a shard recovery from nodeA to nodeB
        final String nodeB = internalCluster().startNode();

        logger.info("--> add replica for {} on node: {}", INDEX_NAME, nodeB);
        execute("ALTER TABLE " + INDEX_NAME + " SET (number_of_replicas=1, \"unassigned.node_left.delayed_timeout\"=0)");
        ensureGreen();

        logger.info("--> start node C");
        final String nodeC = internalCluster().startNode();

        // do sync flush to gen sync id
        execute("OPTIMIZE TABLE " + INDEX_NAME);
        //assertThat(client().admin().indices().prepareSyncedFlush(INDEX_NAME).get().failedShards(), equalTo(0));

        // hold peer recovery on phase 2 after nodeB down
        CountDownLatch phase1ReadyBlocked = new CountDownLatch(1);
        CountDownLatch allowToCompletePhase1Latch = new CountDownLatch(1);
        MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeA);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.CLEAN_FILES.equals(action)) {
                phase1ReadyBlocked.countDown();
                try {
                    allowToCompletePhase1Latch.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        logger.info("--> restart node B");
        internalCluster().restartNode(nodeB,
                                      new InternalTestCluster.RestartCallback() {
                                          @Override
                                          public Settings onNodeStopped(String nodeName) throws Exception {
                                              phase1ReadyBlocked.await();
                                              // nodeB stopped, peer recovery from nodeA to nodeC, it will be cancelled after nodeB get started.
                                              var nodeCRecoveryState = recoveryState(nodeC, INDEX_NAME);

                                              assertOnGoingRecoveryState(nodeCRecoveryState, 0, RecoverySource.Type.PEER,
                                                                         false, nodeA, nodeC);
                                              validateIndexRecoveryState(nodeCRecoveryState);

                                              return super.onNodeStopped(nodeName);
                                          }
                                      });

        // wait for peer recovery from nodeA to nodeB which is a no-op recovery so it skips the CLEAN_FILES stage and hence is not blocked
        ensureGreen();
        allowToCompletePhase1Latch.countDown();
        transportService.clearAllRules();

        // make sure nodeA has primary and nodeB has replica
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        List<ShardRouting> startedShards = state.routingTable().shardsWithState(ShardRoutingState.STARTED);
        assertThat(startedShards.size(), equalTo(2));
        for (ShardRouting shardRouting : startedShards) {
            if (shardRouting.primary()) {
                assertThat(state.nodes().get(shardRouting.currentNodeId()).getName(), equalTo(nodeA));
            } else {
                assertThat(state.nodes().get(shardRouting.currentNodeId()).getName(), equalTo(nodeB));
            }
        }
    }

    @Ignore("CrateDB seems to not expose a shard which is reallocated on the target node until it is fully started." +
            "Thus tracking ongoing recovery fails")
    @Test
    public void testRerouteRecovery() throws Exception {
        logger.info("--> start node A");
        final String nodeA = internalCluster().startNode();

        logger.info("--> create index on node: {}", nodeA);
        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);
        execute("SELECT size FROM sys.shards WHERE table_name = '" + INDEX_NAME + "' AND primary=true");
        long shardSize = (long) response.rows()[0][0];

        logger.info("--> start node B");
        final String nodeB = internalCluster().startNode();

        ensureGreen();

        logger.info("--> slowing down recoveries");
        slowDownRecovery(shardSize);

        logger.info("--> move shard from: {} to: {}", nodeA, nodeB);
        execute("ALTER TABLE " + INDEX_NAME + " REROUTE MOVE SHARD 0 FROM '" + nodeA + "' TO '" + nodeB + "'");

        logger.info("--> waiting for recovery to start both on source and target");
        final Index index = resolveIndex(IndexParts.toIndexName(sqlExecutor.getCurrentSchema(), INDEX_NAME, null));
        assertBusy(() -> {
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeA);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsSource(),
                       equalTo(1));
            indicesService = internalCluster().getInstance(IndicesService.class, nodeB);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsTarget(),
                       equalTo(1));
        });

        logger.info("--> request recoveries");
        var nodeARecoveryState = recoveryState(nodeA, INDEX_NAME);
        assertRecoveryState(nodeARecoveryState, 0, RecoverySource.Type.EMPTY_STORE, true,
                            Stage.DONE, null, nodeA);
        validateIndexRecoveryState(nodeARecoveryState);

        /*
        var nodeBRecoveryState = recoveryState(nodeB, INDEX_NAME);
        assertThat(nodeBRecoveryState, notNullValue());
        assertOnGoingRecoveryState(nodeBRecoveryState, 0, RecoverySource.Type.PEER, true, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryState);

         */

        logger.info("--> request node recovery stats");

        IndicesService indicesServiceNodeA = internalCluster().getInstance(IndicesService.class, nodeA);
        var recoveryStatsNodeA = indicesServiceNodeA.indexServiceSafe(index).getShard(0).recoveryStats();
        assertThat("node A should have ongoing recovery as source", recoveryStatsNodeA.currentAsSource(), equalTo(1));
        assertThat("node A should not have ongoing recovery as target", recoveryStatsNodeA.currentAsTarget(), equalTo(0));
        long nodeAThrottling = recoveryStatsNodeA.throttleTime().millis();

        IndicesService indicesServiceNodeB = internalCluster().getInstance(IndicesService.class, nodeB);
        var recoveryStatsNodeB = indicesServiceNodeB.indexServiceSafe(index).getShard(0).recoveryStats();
        assertThat("node B should not have ongoing recovery as source", recoveryStatsNodeB.currentAsSource(), equalTo(0));
        assertThat("node B should have ongoing recovery as target", recoveryStatsNodeB.currentAsTarget(), equalTo(1));
        long nodeBThrottling = recoveryStatsNodeB.throttleTime().millis();

        logger.info("--> checking throttling increases");
        final long finalNodeAThrottling = nodeAThrottling;
        final long finalNodeBThrottling = nodeBThrottling;
        assertBusy(() -> {
            var recoveryStats = indicesServiceNodeA.indexServiceSafe(index).getShard(0).recoveryStats();
            assertThat("node A throttling should increase", recoveryStats.throttleTime().millis(),
                       greaterThan(finalNodeAThrottling));
            recoveryStats = indicesServiceNodeB.indexServiceSafe(index).getShard(0).recoveryStats();
            assertThat("node B throttling should increase", recoveryStats.throttleTime().millis(),
                       greaterThan(finalNodeBThrottling));
        });


        logger.info("--> speeding up recoveries");
        restoreRecoverySpeed();

        // wait for it to be finished
        ensureGreen();

        /*
        var recoveryState = recoveryState(nodeA, INDEX_NAME);
        assertRecoveryState(recoveryState, 0, RecoverySource.Type.PEER, true, Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(recoveryState);

         */

        Consumer<String> assertNodeHasThrottleTimeAndNoRecoveries = nodeName ->  {
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
            var recoveryStats = indicesService.indexServiceSafe(index).getShard(0).recoveryStats();
            assertThat(recoveryStats.currentAsSource(), equalTo(0));
            assertThat(recoveryStats.currentAsTarget(), equalTo(0));
            assertThat(nodeName + " throttling should be >0", recoveryStats.throttleTime().millis(), greaterThan(0L));
        };
        // we have to use assertBusy as recovery counters are decremented only when the last reference to the RecoveryTarget
        // is decremented, which may happen after the recovery was done.
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeA));
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeB));

        logger.info("--> bump replica count");
        execute("ALTER TABLE " + INDEX_NAME + " SET (number_of_replicas=1)");
        ensureGreen();

        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeA));
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeB));

        logger.info("--> start node C");
        String nodeC = internalCluster().startNode();
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("3").get().isTimedOut());

        logger.info("--> slowing down recoveries");
        slowDownRecovery(shardSize);

        logger.info("--> move replica shard from: {} to: {}", nodeA, nodeC);
        client().admin().cluster().prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, nodeA, nodeC))
            .execute().actionGet().getState();

        nodeARecoveryState = recoveryState(nodeA, INDEX_NAME);
        var nodeBRecoveryState = recoveryState(nodeB, INDEX_NAME);
        var nodeCRecoveryState = recoveryState(nodeC, INDEX_NAME);

        assertRecoveryState(nodeARecoveryState, 0, RecoverySource.Type.PEER, false, Stage.DONE, nodeB, nodeA);
        validateIndexRecoveryState(nodeARecoveryState);

        assertRecoveryState(nodeBRecoveryState, 0, RecoverySource.Type.PEER, true, Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryState);

        // relocations of replicas are marked as REPLICA and the source node is the node holding the primary (B)
        assertOnGoingRecoveryState(nodeCRecoveryState, 0, RecoverySource.Type.PEER, false, nodeB, nodeC);
        validateIndexRecoveryState(nodeCRecoveryState);

        if (randomBoolean()) {
            // shutdown node with relocation source of replica shard and check if recovery continues
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeA));
            ensureStableCluster(2);

            nodeBRecoveryState = recoveryState(nodeB, INDEX_NAME);
            nodeCRecoveryState = recoveryState(nodeC, INDEX_NAME);

            assertRecoveryState(nodeBRecoveryState, 0, RecoverySource.Type.PEER, true, Stage.DONE, nodeA, nodeB);
            validateIndexRecoveryState(nodeBRecoveryState);

            assertOnGoingRecoveryState(nodeCRecoveryState, 0, RecoverySource.Type.PEER, false, nodeB, nodeC);
            validateIndexRecoveryState(nodeCRecoveryState);
        }

        logger.info("--> speeding up recoveries");
        restoreRecoverySpeed();
        ensureGreen();

        nodeARecoveryState = recoveryState(nodeA, INDEX_NAME);
        assertThat(nodeARecoveryState, nullValue());

        nodeBRecoveryState = recoveryState(nodeB, INDEX_NAME);
        nodeCRecoveryState = recoveryState(nodeC, INDEX_NAME);

        assertRecoveryState(nodeBRecoveryState, 0, RecoverySource.Type.PEER, true, Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryState);

        // relocations of replicas are marked as REPLICA and the source node is the node holding the primary (B)
        assertRecoveryState(nodeCRecoveryState, 0, RecoverySource.Type.PEER, false, Stage.DONE, nodeB, nodeC);
        validateIndexRecoveryState(nodeCRecoveryState);
    }

    @Test
    public void testSnapshotRecovery() throws Exception {
        logger.info("--> start node A");
        String nodeA = internalCluster().startNode();

        logger.info("--> create repository");
        execute("CREATE REPOSITORY " + REPO_NAME + " TYPE FS WITH (location = '" + randomRepoPath() + "', compress=false)");

        ensureGreen();

        logger.info("--> create index on node: {}", nodeA);
        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> snapshot");
        var snapshotName = REPO_NAME + "." + SNAP_NAME;
        execute("CREATE SNAPSHOT " + snapshotName + " ALL WITH (wait_for_completion=true)");

        execute("SELECT state FROM sys.snapshots WHERE name = '" + SNAP_NAME + "'");
        assertThat(response.rows()[0][0], is("SUCCESS"));

        execute("ALTER TABLE " + INDEX_NAME + " CLOSE");

        logger.info("--> restore");
        execute("RESTORE SNAPSHOT " + snapshotName + " ALL WITH (wait_for_completion=true)");

        ensureGreen();

        logger.info("--> request recoveries");
        var recoveryState = recoveryState(nodeA, INDEX_NAME);
        assertRecoveryState(recoveryState, 0, RecoverySource.Type.SNAPSHOT, true, Stage.DONE, null, nodeA);
        validateIndexRecoveryState(recoveryState);
    }

    @Test
    public void testTransientErrorsDuringRecoveryAreRetried() throws Exception {
        final String indexName = "test";
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "500ms")
            .put(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.getKey(), "10s")
            .build();
        // start a master node
        internalCluster().startNode(nodeSettings);

        final String blueNodeName = internalCluster()
            .startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        final String redNodeName = internalCluster()
            .startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));

        execute("CREATE TABLE doc." + indexName + " (id int) CLUSTERED INTO 1 SHARDS " +
                "WITH (" +
                " number_of_replicas=0," +
                " \"routing.allocation.include.color\" = 'blue'" +
                ")");

        int numDocs = scaledRandomIntBetween(100, 8000);
        // Index 3/4 of the documents and flush. And then index the rest. This attempts to ensure that there
        // is a mix of file chunks and translog ops
        int threeFourths = (int) (numDocs * 0.75);
        var args = new Object[threeFourths][];
        for (int i = 0; i < threeFourths; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO doc." + indexName + " (id) VALUES (?)", args);
        execute("OPTIMIZE TABLE doc." + indexName);

        args = new Object[numDocs - threeFourths][];
        for (int i = 0; i < numDocs-threeFourths; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO doc." + indexName + " (id) VALUES (?)", args);
        ensureGreen();

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        final String blueNodeId = internalCluster().getInstance(ClusterService.class, blueNodeName).localNode().getId();

        assertFalse(stateResponse.getState().getRoutingNodes().node(blueNodeId).isEmpty());

        refresh();
        var searchResponse = execute("SELECT COUNT(*) FROM doc." + indexName);
        assertThat((long) searchResponse.rows()[0][0], is((long) numDocs));

        String[] recoveryActions = new String[]{
            PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG,
            PeerRecoveryTargetService.Actions.TRANSLOG_OPS,
            PeerRecoveryTargetService.Actions.FILES_INFO,
            PeerRecoveryTargetService.Actions.FILE_CHUNK,
            PeerRecoveryTargetService.Actions.CLEAN_FILES,
            PeerRecoveryTargetService.Actions.FINALIZE
        };
        final String recoveryActionToBlock = randomFrom(recoveryActions);
        logger.info("--> will temporarily interrupt recovery action between blue & red on [{}]", recoveryActionToBlock);

        MockTransportService blueTransportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, blueNodeName);
        MockTransportService redTransportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, redNodeName);

        final AtomicBoolean recoveryStarted = new AtomicBoolean(false);
        final AtomicBoolean finalizeReceived = new AtomicBoolean(false);

        final SingleStartEnforcer validator = new SingleStartEnforcer(indexName, recoveryStarted, finalizeReceived);
        redTransportService.addSendBehavior(blueTransportService, (connection, requestId, action, request, options) -> {
            validator.accept(action, request);
            connection.sendRequest(requestId, action, request, options);
        });
        Runnable connectionBreaker = () -> {
            // Always break connection from source to remote to ensure that actions are retried
            logger.info("--> closing connections from source node to target node");
            blueTransportService.disconnectFromNode(redTransportService.getLocalNode());
            if (randomBoolean()) {
                // Sometimes break connection from remote to source to ensure that recovery is re-established
                logger.info("--> closing connections from target node to source node");
                redTransportService.disconnectFromNode(blueTransportService.getLocalNode());
            }
        };
        TransientReceiveRejected handlingBehavior =
            new TransientReceiveRejected(recoveryActionToBlock, finalizeReceived, recoveryStarted, connectionBreaker);
        redTransportService.addRequestHandlingBehavior(recoveryActionToBlock, handlingBehavior);

        try {
            logger.info("--> starting recovery from blue to red");
            execute("ALTER TABLE doc." + indexName + " SET (" +
                    " number_of_replicas=1," +
                    " \"routing.allocation.include.color\" = 'red,blue'" +
                    ")");

            ensureGreen();
            // TODO: ES will use the shard routing preference here to prefer `_local` shards on that node
            var nodeRedExecutor = executor(redNodeName);
            searchResponse = nodeRedExecutor.exec("SELECT COUNT(*) FROM doc." + indexName);
            assertThat((long) searchResponse.rows()[0][0], is((long) numDocs));
        } finally {
            blueTransportService.clearAllRules();
            redTransportService.clearAllRules();
        }
    }

    private class TransientReceiveRejected implements StubbableTransport.RequestHandlingBehavior<TransportRequest> {

        private final String actionName;
        private final AtomicBoolean recoveryStarted;
        private final AtomicBoolean finalizeReceived;
        private final Runnable connectionBreaker;
        private final AtomicInteger blocksRemaining;

        private TransientReceiveRejected(String actionName, AtomicBoolean recoveryStarted, AtomicBoolean finalizeReceived,
                                         Runnable connectionBreaker) {
            this.actionName = actionName;
            this.recoveryStarted = recoveryStarted;
            this.finalizeReceived = finalizeReceived;
            this.connectionBreaker = connectionBreaker;
            this.blocksRemaining = new AtomicInteger(randomIntBetween(1, 3));
        }

        @Override
        public void messageReceived(TransportRequestHandler<TransportRequest> handler,
                                    TransportRequest request,
                                    TransportChannel channel) throws Exception {
            recoveryStarted.set(true);
            if (actionName.equals(PeerRecoveryTargetService.Actions.FINALIZE)) {
                finalizeReceived.set(true);
            }
            if (blocksRemaining.getAndUpdate(i -> i == 0 ? 0 : i - 1) != 0) {
                String rejected = "rejected";
                String circuit = "circuit";
                String network = "network";
                String reason = randomFrom(rejected, circuit, network);
                if (reason.equals(rejected)) {
                    logger.info("--> preventing {} response by throwing exception", actionName);
                    throw new EsRejectedExecutionException("foo", false);
                } else if (reason.equals(circuit)) {
                    logger.info("--> preventing {} response by throwing exception", actionName);
                    throw new CircuitBreakingException("Broken");
                } else if (reason.equals(network)) {
                    logger.info("--> preventing {} response by breaking connection", actionName);
                    connectionBreaker.run();
                } else {
                    throw new AssertionError("Unknown failure reason: " + reason);
                }
            }
            handler.messageReceived(request, channel);
        }
    }

    private class SingleStartEnforcer implements BiConsumer<String, TransportRequest> {

        private final AtomicBoolean recoveryStarted;
        private final AtomicBoolean finalizeReceived;
        private final String indexName;

        private SingleStartEnforcer(String indexName, AtomicBoolean recoveryStarted, AtomicBoolean finalizeReceived) {
            this.indexName = indexName;
            this.recoveryStarted = recoveryStarted;
            this.finalizeReceived = finalizeReceived;
        }

        @Override
        public void accept(String action, TransportRequest request) {
            // The cluster state applier will immediately attempt to retry the recovery on a cluster state
            // update. We want to assert that the first and only recovery attempt succeeds
            if (PeerRecoverySourceService.Actions.START_RECOVERY.equals(action)) {
                StartRecoveryRequest startRecoveryRequest = (StartRecoveryRequest) request;
                ShardId shardId = startRecoveryRequest.shardId();
                logger.info("--> attempting to send start_recovery request for shard: " + shardId);
                if (indexName.equals(shardId.getIndexName()) && recoveryStarted.get() && finalizeReceived.get() == false) {
                    throw new IllegalStateException("Recovery cannot be started twice");
                }
            }
        }
    }

    @Test
    public void testDisconnectsWhileRecovering() throws Exception {
        final String indexName = "test";
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
            .put(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.getKey(), "1s")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "1s")
            .build();
        // start a master node
        internalCluster().startNode(nodeSettings);

        final String blueNodeName = internalCluster()
            .startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        final String redNodeName = internalCluster()
            .startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));

        execute("CREATE TABLE doc." + indexName + " (id int) CLUSTERED INTO 1 SHARDS " +
                "WITH (" +
                " number_of_replicas=0," +
                " \"routing.allocation.include.color\" = 'blue'" +
                ")");

        int numDocs = scaledRandomIntBetween(25, 250);
        var args = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO doc." + indexName + " (id) VALUES (?)", args);
        ensureGreen();

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        final String blueNodeId = internalCluster().getInstance(ClusterService.class, blueNodeName).localNode().getId();

        assertFalse(stateResponse.getState().getRoutingNodes().node(blueNodeId).isEmpty());

        refresh();
        var searchResponse = execute("SELECT COUNT(*) FROM doc." + indexName);
        assertThat((long) searchResponse.rows()[0][0], is((long) numDocs));

        String[] recoveryActions = new String[]{
            PeerRecoverySourceService.Actions.START_RECOVERY,
            PeerRecoveryTargetService.Actions.FILES_INFO,
            PeerRecoveryTargetService.Actions.FILE_CHUNK,
            PeerRecoveryTargetService.Actions.CLEAN_FILES,
            //RecoveryTarget.Actions.TRANSLOG_OPS, <-- may not be sent if already flushed
            PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG,
            PeerRecoveryTargetService.Actions.FINALIZE
        };
        final String recoveryActionToBlock = randomFrom(recoveryActions);
        final boolean dropRequests = randomBoolean();
        logger.info("--> will {} between blue & red on [{}]", dropRequests ? "drop requests" : "break connection", recoveryActionToBlock);

        MockTransportService blueMockTransportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, blueNodeName);
        MockTransportService redMockTransportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, redNodeName);
        TransportService redTransportService = internalCluster().getInstance(TransportService.class, redNodeName);
        TransportService blueTransportService = internalCluster().getInstance(TransportService.class, blueNodeName);
        final CountDownLatch requestFailed = new CountDownLatch(1);

        if (randomBoolean()) {
            // Fail on the sending side
            blueMockTransportService.addSendBehavior(redTransportService,
                                                     new RecoveryActionBlocker(dropRequests, recoveryActionToBlock, requestFailed));
            redMockTransportService.addSendBehavior(blueTransportService,
                                                    new RecoveryActionBlocker(dropRequests, recoveryActionToBlock, requestFailed));
        } else {
            // Fail on the receiving side.
            blueMockTransportService.addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel) -> {
                logger.info("--> preventing {} response by closing response channel", recoveryActionToBlock);
                requestFailed.countDown();
                redMockTransportService.disconnectFromNode(blueMockTransportService.getLocalNode());
                handler.messageReceived(request, channel);
            });
            redMockTransportService.addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel) -> {
                logger.info("--> preventing {} response by closing response channel", recoveryActionToBlock);
                requestFailed.countDown();
                blueMockTransportService.disconnectFromNode(redMockTransportService.getLocalNode());
                handler.messageReceived(request, channel);
            });
        }

        logger.info("--> starting recovery from blue to red");
        execute("ALTER TABLE doc." + indexName + " SET (" +
                " number_of_replicas=1," +
                " \"routing.allocation.include.color\" = 'red,blue'" +
                ")");

        requestFailed.await();

        logger.info("--> clearing rules to allow recovery to proceed");
        blueMockTransportService.clearAllRules();
        redMockTransportService.clearAllRules();

        ensureGreen();
        // TODO: ES will use the shard routing preference here to prefer `_local` shards on that node
        var nodeRedExecutor = executor(redNodeName);
        searchResponse = nodeRedExecutor.exec("SELECT COUNT(*) FROM doc." + indexName);
        assertThat((long) searchResponse.rows()[0][0], is((long) numDocs));
    }

    private class RecoveryActionBlocker implements StubbableTransport.SendRequestBehavior {
        private final boolean dropRequests;
        private final String recoveryActionToBlock;
        private final CountDownLatch requestBlocked;

        RecoveryActionBlocker(boolean dropRequests, String recoveryActionToBlock, CountDownLatch requestBlocked) {
            this.dropRequests = dropRequests;
            this.recoveryActionToBlock = recoveryActionToBlock;
            this.requestBlocked = requestBlocked;
        }

        @Override
        public void sendRequest(Transport.Connection connection, long requestId, String action, TransportRequest request,
                                TransportRequestOptions options) throws IOException {
            if (recoveryActionToBlock.equals(action) || requestBlocked.getCount() == 0) {
                requestBlocked.countDown();
                if (dropRequests) {
                    logger.info("--> preventing {} request by dropping request", action);
                    return;
                } else {
                    logger.info("--> preventing {} request by throwing ConnectTransportException", action);
                    throw new ConnectTransportException(connection.getNode(), "DISCONNECT: prevented " + action + " request");
                }
            }
            connection.sendRequest(requestId, action, request, options);
        }
    }

    /**
     * Tests scenario where recovery target successfully sends recovery request to source but then the channel gets closed while
     * the source is working on the recovery process.
     */
    @Test
    public void testDisconnectsDuringRecovery() throws Exception {
        boolean primaryRelocation = randomBoolean();
        final String indexName = IndexParts.toIndexName(sqlExecutor.getCurrentSchema(), "test", null);
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(),
                 TimeValue.timeValueMillis(randomIntBetween(0, 100)))
            .build();
        TimeValue disconnectAfterDelay = TimeValue.timeValueMillis(randomIntBetween(0, 100));
        // start a master node
        String masterNodeName = internalCluster().startMasterOnlyNode(nodeSettings);

        final String blueNodeName = internalCluster()
            .startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        final String redNodeName = internalCluster()
            .startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        execute("CREATE TABLE test (id int) CLUSTERED INTO 1 SHARDS " +
                "WITH (" +
                " number_of_replicas=0," +
                " \"routing.allocation.include.color\" = 'blue'" +
                ")");

        int numDocs = scaledRandomIntBetween(25, 250);
        var args = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO test (id) VALUES (?)", args);
        ensureGreen();

        refresh();
        var searchResponse = execute("SELECT COUNT(*) FROM test");
        assertThat((long) searchResponse.rows()[0][0], is((long) numDocs));

        MockTransportService masterTransportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, masterNodeName);
        MockTransportService blueMockTransportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, blueNodeName);
        MockTransportService redMockTransportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, redNodeName);

        redMockTransportService.addSendBehavior(blueMockTransportService, new StubbableTransport.SendRequestBehavior() {
            private final AtomicInteger count = new AtomicInteger();

            @Override
            public void sendRequest(Transport.Connection connection, long requestId, String action, TransportRequest request,
                                    TransportRequestOptions options) throws IOException {
                logger.info("--> sending request {} on {}", action, connection.getNode());
                if (PeerRecoverySourceService.Actions.START_RECOVERY.equals(action) && count.incrementAndGet() == 1) {
                    // ensures that it's considered as valid recovery attempt by source
                    try {
                        assertBusy(() -> assertThat(
                            "Expected there to be some initializing shards",
                            client(blueNodeName).admin().cluster().prepareState().setLocal(true).get()
                                .getState().getRoutingTable().index(indexName).shard(0).getAllInitializingShards(), not(empty())));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    connection.sendRequest(requestId, action, request, options);
                    try {
                        Thread.sleep(disconnectAfterDelay.millis());
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    throw new ConnectTransportException(connection.getNode(),
                                                        "DISCONNECT: simulation disconnect after successfully sending " + action + " request");
                } else {
                    connection.sendRequest(requestId, action, request, options);
                }
            }
        });

        final AtomicBoolean finalized = new AtomicBoolean();
        blueMockTransportService.addSendBehavior(redMockTransportService, (connection, requestId, action, request, options) -> {
            logger.info("--> sending request {} on {}", action, connection.getNode());
            if (action.equals(PeerRecoveryTargetService.Actions.FINALIZE)) {
                finalized.set(true);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        for (MockTransportService mockTransportService : Arrays.asList(redMockTransportService, blueMockTransportService)) {
            mockTransportService.addSendBehavior(masterTransportService, (connection, requestId, action, request, options) -> {
                logger.info("--> sending request {} on {}", action, connection.getNode());
                if ((primaryRelocation && finalized.get()) == false) {
                    assertNotEquals(action, ShardStateAction.SHARD_FAILED_ACTION_NAME);
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        if (primaryRelocation) {
            logger.info("--> starting primary relocation recovery from blue to red");
            execute("ALTER TABLE test SET (" +
                    " \"routing.allocation.include.color\" = 'red'" +
                    ")");

            ensureGreen(); // also waits for relocation / recovery to complete
            // if a primary relocation fails after the source shard has been marked as relocated, both source and target are failed. If the
            // source shard is moved back to started because the target fails first, it's possible that there is a cluster state where the
            // shard is marked as started again (and ensureGreen returns), but while applying the cluster state the primary is failed and
            // will be reallocated. The cluster will thus become green, then red, then green again. Triggering a refresh here before
            // searching helps, as in contrast to search actions, refresh waits for the closed shard to be reallocated.
            refresh();
        } else {
            logger.info("--> starting replica recovery from blue to red");
            execute("ALTER TABLE test SET (" +
                    " number_of_replicas=1," +
                    " \"routing.allocation.include.color\" = 'red,blue'" +
                    ")");

            ensureGreen();
        }

        for (int i = 0; i < 10; i++) {
            searchResponse = execute("SELECT COUNT(*) FROM test");
            assertThat((long) searchResponse.rows()[0][0], is((long) numDocs));
        }
    }

    @Test
    public void testHistoryRetention() throws Exception {
        internalCluster().startNodes(3);

        final String indexName = IndexParts.toIndexName(sqlExecutor.getCurrentSchema(), "test", null);
        execute("CREATE TABLE test (id int) CLUSTERED INTO 1 SHARDS " +
                "WITH (" +
                " number_of_replicas=2," +
                " \"recovery.file_based_threshold\" = 1.0" +
                ")");

        // Perform some replicated operations so the replica isn't simply empty, because ops-based recovery isn't better in that case
        int numDocs = scaledRandomIntBetween(25, 250);
        var args = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO test (id) VALUES (?)", args);
        if (randomBoolean()) {
            execute("OPTIMIZE TABLE test");
        }
        ensureGreen();


        String firstNodeToStop = randomFrom(internalCluster().getNodeNames());
        Settings firstNodeToStopDataPathSettings = internalCluster().dataPathSettings(firstNodeToStop);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(firstNodeToStop));
        String secondNodeToStop = randomFrom(internalCluster().getNodeNames());
        Settings secondNodeToStopDataPathSettings = internalCluster().dataPathSettings(secondNodeToStop);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(secondNodeToStop));

        final long desyncNanoTime = System.nanoTime();
        //noinspection StatementWithEmptyBody
        while (System.nanoTime() <= desyncNanoTime) {
            // time passes
        }

        final int numNewDocs = scaledRandomIntBetween(25, 250);
        args = new Object[numNewDocs][];
        for (int i = 0; i < numNewDocs; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO test (id) VALUES (?)", args);
        refresh();
        // Flush twice to update the safe commit's local checkpoint
        execute("OPTIMIZE TABLE test");
        execute("OPTIMIZE TABLE test");

        execute("ALTER TABLE test SET (number_of_replicas = 1)");
        internalCluster().startNode(randomFrom(firstNodeToStopDataPathSettings, secondNodeToStopDataPathSettings));
        ensureGreen(indexName);

        final RecoveryResponse recoveryResponse = client().admin().indices().recoveries(new RecoveryRequest(indexName)).get();
        final List<org.elasticsearch.indices.recovery.RecoveryState> recoveryStates = recoveryResponse.shardRecoveryStates().get(indexName);
        recoveryStates.removeIf(r -> r.getTimer().getStartNanoTime() <= desyncNanoTime);

        assertThat(recoveryStates, hasSize(1));
        assertThat(recoveryStates.get(0).getIndex().totalFileCount(), is(0));
        assertThat(recoveryStates.get(0).getTranslog().recoveredOperations(), greaterThan(0));
    }

    @Test
    public void testDoNotInfinitelyWaitForMapping() {
        internalCluster().ensureAtLeastNumDataNodes(3);
        execute("CREATE ANALYZER test_analyzer (" +
                " TOKENIZER standard," +
                " TOKEN_FILTERS (test_token_filter)" +
                ")");
        execute("CREATE TABLE test (test_field TEXT INDEX USING FULLTEXT WITH (ANALYZER='test_analyzer'))" +
                " CLUSTERED INTO 1 SHARDS WITH (number_of_replicas = 0)");

        int numDocs = between(1, 10);
        var args = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            args[i] = new Object[]{Integer.toString(i)};
        }
        execute("INSERT INTO test (test_field) VALUES (?)", args);

        Semaphore recoveryBlocked = new Semaphore(1);
        for (DiscoveryNode node : clusterService().state().nodes()) {
            MockTransportService transportService = (MockTransportService) internalCluster().getInstance(
                TransportService.class, node.getName());
            transportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(PeerRecoverySourceService.Actions.START_RECOVERY)) {
                    if (recoveryBlocked.tryAcquire()) {
                        PluginsService pluginService = internalCluster().getInstance(PluginsService.class, node.getName());
                        for (TestAnalysisPlugin plugin : pluginService.filterPlugins(TestAnalysisPlugin.class)) {
                            plugin.throwParsingError.set(true);
                        }
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }
        execute("ALTER TABLE test SET (number_of_replicas = 1)");
        ensureGreen();
        refresh();
        var searchResponse = execute("SELECT COUNT(*) FROM test");
        assertThat((long) searchResponse.rows()[0][0], is((long) numDocs));
    }

    public static final class TestAnalysisPlugin extends Plugin implements AnalysisPlugin {
        final AtomicBoolean throwParsingError = new AtomicBoolean();
        @Override
        public Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
            return singletonMap("test_token_filter",
                                (indexSettings, environment, name, settings) -> new AbstractTokenFilterFactory(indexSettings, name, settings) {
                                    @Override
                                    public TokenStream create(TokenStream tokenStream) {
                                        if (throwParsingError.get()) {
                                            throw new MapperParsingException("simulate mapping parsing error");
                                        }
                                        return tokenStream;
                                    }
                                });
        }
    }
}
