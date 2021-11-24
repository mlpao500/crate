/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.protocols.postgres;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.junit.Test;

import io.crate.action.FutureActionListener;
import io.crate.action.sql.SQLOperations;
import io.crate.auth.AlwaysOKAuthentication;
import io.crate.auth.Authentication;
import io.crate.netty.NettyBootstrap;
import io.crate.protocols.ssl.SslContextProvider;
import io.crate.replication.logical.action.PublicationsStateAction;
import io.crate.user.StubUserManager;
import io.crate.user.User;

public class PgClientTest {

    @Test
    public void test_pg_client_can_connect_to_postgres_netty() throws Exception {
        var serverNodeSettings = Settings.builder()
            .put("node.name", "server")
            .build();
        var clientSettings = Settings.builder()
            .put("node.name", "client")
            .build();
        var nettyBootstrap = new NettyBootstrap();
        var pageCacheRecycler = PageCacheRecycler.NON_RECYCLING_INSTANCE;
        var networkService = new NetworkService(List.of());
        var namedWriteableRegistry = new NamedWriteableRegistry(List.of());
        var circuitBreakerService = new NoneCircuitBreakerService();
        var threadPool = new ThreadPool(serverNodeSettings);
        Authentication authentication = new AlwaysOKAuthentication(ignored -> User.CRATE_USER);
        var sslContextProvider = new SslContextProvider(serverNodeSettings);
        var serverTransport = new Netty4Transport(
            serverNodeSettings,
            Version.CURRENT,
            threadPool,
            networkService,
            pageCacheRecycler,
            namedWriteableRegistry,
            circuitBreakerService,
            nettyBootstrap,
            authentication,
            sslContextProvider
        );
        var clientTransport = new Netty4Transport(
            clientSettings,
            Version.CURRENT,
            threadPool,
            networkService,
            pageCacheRecycler,
            namedWriteableRegistry,
            circuitBreakerService,
            nettyBootstrap,
            authentication,
            sslContextProvider
        );
        PostgresNetty postgresNetty = new PostgresNetty(
            serverNodeSettings,
            mock(SQLOperations.class),
            new StubUserManager(),
            networkService,
            authentication,
            nettyBootstrap,
            serverTransport,
            pageCacheRecycler,
            sslContextProvider
        );
        postgresNetty.start();
        TransportAddress serverAddress = postgresNetty.boundAddress().publishAddress();
        DiscoveryNode serverHost = new DiscoveryNode("server", serverAddress, Version.CURRENT);
        DiscoveryNode localNode = new DiscoveryNode("client", serverAddress, Version.CURRENT);
        var clientTransportService = new TransportService(
            clientSettings,
            clientTransport,
            threadPool,
            address -> localNode,
            null
        );
        var pgClient = new PgClient(
            clientSettings,
            clientTransportService,
            nettyBootstrap,
            clientTransport,
            pageCacheRecycler,
            serverHost
        );

        CompletableFuture<Connection> connect = pgClient.connect();
        Connection connection = connect.get(120, TimeUnit.SECONDS);
        System.out.println(connection);
        assertThat(connection.getNode(), is(serverHost));

        // TODO: actions are not registered
        Client remoteClient = pgClient.getRemoteClient(connection);
        // FutureActionListener<PublicationsStateAction.Response, PublicationsStateAction.Response> listener = FutureActionListener.newInstance();
        // client.execute(
        //     PublicationsStateAction.INSTANCE,
        //     new PublicationsStateAction.Request(List.of("pub1"), "crate"),
        //     listener
        // );

        // io.crate.replication.logical.action.PublicationsStateAction.Response response = listener.get(5, TimeUnit.SECONDS);

        // System.out.println(response);

        postgresNetty.close();
        pgClient.close();
        remoteClient.close();
    }
}
