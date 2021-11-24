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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.CloseableConnection;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.OutboundHandler;
import org.elasticsearch.transport.RemoteClusterAwareRequest;
import org.elasticsearch.transport.RemoteConnectionManager.ProxyConnection;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.transport.netty4.Netty4MessageChannelHandler;
import org.elasticsearch.transport.netty4.Netty4TcpChannel;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.transport.netty4.Netty4Utils;

import io.crate.common.collections.BorrowedItem;
import io.crate.netty.NettyBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class PgClient implements Closeable {

    private final Settings settings;
    private final NettyBootstrap nettyBootstrap;
    private final Netty4Transport transport;
    private final PageCacheRecycler pageCacheRecycler;
    private final DiscoveryNode host;
    private final TransportService transportService;

    private BorrowedItem<EventLoopGroup> eventLoopGroup;
    private Bootstrap bootstrap;
    private Channel channel;


    public PgClient(Settings nodeSettings,
                    TransportService transportService,
                    NettyBootstrap nettyBootstrap,
                    Netty4Transport transport,
                    PageCacheRecycler pageCacheRecycler,
                    DiscoveryNode host) {
        this.settings = nodeSettings;
        this.transportService = transportService;
        this.nettyBootstrap = nettyBootstrap;
        this.transport = transport;
        this.pageCacheRecycler = pageCacheRecycler;
        this.host = host;
    }

    public CompletableFuture<Transport.Connection> connect() {
        bootstrap = new Bootstrap();
        eventLoopGroup = nettyBootstrap.getSharedEventLoopGroup(settings);
        bootstrap.group(eventLoopGroup.item());
        bootstrap.channel(NettyBootstrap.clientChannel());
        bootstrap.option(ChannelOption.TCP_NODELAY, TransportSettings.TCP_NO_DELAY.get(settings));
        bootstrap.option(ChannelOption.SO_KEEPALIVE, TransportSettings.TCP_KEEP_ALIVE.get(settings));

        CompletableFuture<Transport.Connection> result = new CompletableFuture<>();
        bootstrap.handler(new ClientChannelInitializer(
            settings,
            host,
            transport,
            pageCacheRecycler,
            result
        ));
        bootstrap.remoteAddress(host.getAddress().address());
        ChannelFuture connectFuture = bootstrap.connect();
        channel = connectFuture.channel();
        Netty4TcpChannel nettyChannel = new Netty4TcpChannel(
            channel,
            false,
            "default",
            connectFuture
        );
        channel.attr(Netty4Transport.CHANNEL_KEY).set(nettyChannel);

        ByteBuf buffer = channel.alloc().buffer();
        /// TODO: user must come from connectionInfo
        ClientMessages.sendStartupMessage(buffer, "doc", Map.of("user", "crate", "CrateDBTransport", "true"));
        channel.writeAndFlush(buffer);
        return result;
    }

    @Override
    public void close() throws IOException {
        if (bootstrap != null) {
            bootstrap = null;
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.close();
            eventLoopGroup = null;
        }
        if (channel != null) {
            Netty4Utils.closeChannels(List.of(channel));
            channel = null;
        }
    }

    static class ClientChannelInitializer extends ChannelInitializer<Channel> {

        private final Settings settings;
        private final DiscoveryNode node;
        private final Netty4Transport transport;
        private final CompletableFuture<Transport.Connection> result;
        private final PageCacheRecycler pageCacheRecycler;

        public ClientChannelInitializer(Settings settings,
                                        DiscoveryNode node,
                                        Netty4Transport transport,
                                        PageCacheRecycler pageCacheRecycler,
                                        CompletableFuture<Transport.Connection> result) {
            this.settings = settings;
            this.node = node;
            this.transport = transport;
            this.pageCacheRecycler = pageCacheRecycler;
            this.result = result;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("decoder", new Decoder());

            Handler handler = new Handler(settings, node, transport, pageCacheRecycler, result);
            ch.pipeline().addLast("dispatcher", handler);
        }
    }

    static class Handler extends SimpleChannelInboundHandler<ByteBuf> {

        private final CompletableFuture<Transport.Connection> result;
        private final PageCacheRecycler pageCacheRecycler;
        private final Settings settings;
        private final DiscoveryNode node;
        private final Netty4Transport transport;

        public Handler(Settings settings,
                       DiscoveryNode node,
                       Netty4Transport transport,
                       PageCacheRecycler pageCacheRecycler,
                       CompletableFuture<Transport.Connection> result) {
            this.settings = settings;
            this.node = node;
            this.transport = transport;
            this.pageCacheRecycler = pageCacheRecycler;
            this.result = result;
        }

        @Override
        public boolean acceptInboundMessage(Object msg) throws Exception {
            return true;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            byte msgType = msg.readByte();
            msg.readInt(); // msgLength

            switch (msgType) {
                case 'R' -> handleAuth(msg);
                case 'S' -> handleParameterStatus(msg);
                case 'Z' -> handleReadyForQuery(ctx.channel(), msg);
                default -> throw new IllegalStateException("Unexpected message type: " + msgType);
            }
        }

        private void handleReadyForQuery(Channel channel, ByteBuf msg) {
            byte transactionStatus = msg.readByte();
            System.out.println("transactionStatus=" + (char) transactionStatus);

            channel.pipeline().remove("decoder");
            channel.pipeline().remove("dispatcher");

            // Internally this handler uses `ctx.channel().attr(Netty4Transport.CHANNEL_KEY)` as channel for communication
            // So it won't use transport channel, but uses channels from *this* pipeline
            var handler = new Netty4MessageChannelHandler(pageCacheRecycler, transport);
            channel.pipeline().addLast("dispatcher", handler);

            Netty4TcpChannel tcpChannel = channel.attr(Netty4Transport.CHANNEL_KEY).get();
            ConnectionProfile connectionProfile = new ConnectionProfile.Builder()
                .setConnectTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings))
                .setHandshakeTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings))
                .setPingInterval(TransportSettings.PING_SCHEDULE.get(settings))
                .setCompressionEnabled(TransportSettings.TRANSPORT_COMPRESS.get(settings))
                .addConnections(1, TransportRequestOptions.Type.BULK)
                .addConnections(1, TransportRequestOptions.Type.PING)
                .addConnections(1, TransportRequestOptions.Type.STATE)
                .addConnections(1, TransportRequestOptions.Type.RECOVERY)
                .addConnections(1, TransportRequestOptions.Type.REG)
                .build();
            ActionListener<Version> onHandshakeResponse = ActionListener.wrap(
                version -> {
                    var connection = new TunneledConnection(
                        transport.outboundHandler(),
                        node,
                        tcpChannel,
                        connectionProfile,
                        version
                    );
                    long relativeMillisTime = this.transport.getThreadPool().relativeTimeInMillis();
                    tcpChannel.getChannelStats().markAccessed(relativeMillisTime);
                    tcpChannel.addCloseListener(ActionListener.wrap(connection::close));
                    result.complete(connection);
                },
                e -> {
                    var error = e instanceof ConnectTransportException ? e : new ConnectTransportException(node, "general node connection failure", e);
                    try {
                        CloseableChannel.closeChannels(List.of(tcpChannel), false);
                    } catch (Exception ex) {
                        error.addSuppressed(ex);
                    } finally {
                        result.completeExceptionally(error);
                    }
                }
            );
            transport.executeHandshake(node, tcpChannel, connectionProfile, onHandshakeResponse);
        }

        private void handleParameterStatus(ByteBuf msg) {
            String name = PostgresWireProtocol.readCString(msg);
            String value = PostgresWireProtocol.readCString(msg);
            System.out.println(name + "=" + value);
        }

        private void handleAuth(ByteBuf msg) {
            AuthType authType = AuthType.of(msg.readInt());
            System.out.println("AuthType=" + authType);
            switch (authType) {
                case Ok:
                    break;
                case CleartextPassword:
                    break;
                case KerberosV5:
                    break;
                case MD5Password:
                    break;
                default:
                    break;
            }
        }
    }

    enum AuthType {
        Ok,
        KerberosV5,
        CleartextPassword,
        MD5Password;

        public static AuthType of(int type) {
            return switch (type) {
                case 0 -> Ok;
                case 2 -> KerberosV5;
                case 3 -> CleartextPassword;
                case 5 -> MD5Password;
                default -> throw new IllegalArgumentException("Unknown auth type: " + type);
            };
        }
    }


    static class Decoder extends LengthFieldBasedFrameDecoder {

        // PostgreSQL wire protocol message format:
        // | Message Type (Byte1) | Length including self (Int32) | Body (depending on type) |
        private static final int LENGTH_FIELD_OFFSET = 1;
        private static final int LENGTH_FIELD_LENGTH = 4;
        private static final int LENGTH_ADJUSTMENT = -4;

        // keep the header
        private static final int INITIAL_BYTES_TO_STRIP = 0;

        public Decoder() {
            super(
                Integer.MAX_VALUE,
                LENGTH_FIELD_OFFSET,
                LENGTH_FIELD_LENGTH,
                LENGTH_ADJUSTMENT,
                INITIAL_BYTES_TO_STRIP
            );
        }
    }


    public Client getRemoteClient(Transport.Connection connection) {
        return new TransportPgClient(
            settings,
            transportService,
            transportService.getThreadPool(),
            connection
        );
    }

    public static class TunneledConnection extends CloseableConnection {

        private final DiscoveryNode node;
        private final Netty4TcpChannel channel;
        private final ConnectionProfile connectionProfile;
        private final Version version;
        private final OutboundHandler outboundHandler;
        private final AtomicBoolean isClosing = new AtomicBoolean(false);

        public TunneledConnection(OutboundHandler outboundHandler,
                                  DiscoveryNode node,
                                  Netty4TcpChannel channel,
                                  ConnectionProfile connectionProfile,
                                  Version version) {
            this.outboundHandler = outboundHandler;
            this.node = node;
            this.channel = channel;
            this.connectionProfile = connectionProfile;
            this.version = version;
        }

        @Override
        public DiscoveryNode getNode() {
            return node;
        }

        @Override
        public void sendRequest(long requestId,
                                String action,
                                TransportRequest request,
                                TransportRequestOptions options) throws IOException, TransportException {
            if (isClosing.get()) {
                throw new NodeNotConnectedException(node, "connection already closed");
            }
            outboundHandler.sendRequest(
                node,
                channel,
                requestId,
                action,
                request,
                options,
                version,
                connectionProfile.getCompressionEnabled(),
                false
            );
        }


        @Override
        public void close() {
            if (isClosing.compareAndSet(false, true)) {
                try {
                    CloseableChannel.closeChannels(List.of(channel), false);
                } finally {
                    super.close();
                }
            }
        }
    }

    private final class TransportPgClient extends AbstractClient {
        private final Transport.Connection connection;
        private final TransportService transportService;

        private TransportPgClient(Settings settings,
                                  TransportService transportService,
                                  ThreadPool threadPool,
                                  Transport.Connection connection) {
            super(settings, threadPool);
            this.transportService = transportService;
            this.connection = connection;
        }

        protected <Request extends TransportRequest, Response extends TransportResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener) {

            // TODO: Remove, this is temporary to be able to set breakpoints
            var wrappedListener = new ActionListener<Response>() {

                @Override
                public void onResponse(Response response) {
                    listener.onResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    System.out.println("##################");
                    e.printStackTrace();
                    listener.onFailure(e);
                }
            };

            if (request instanceof RemoteClusterAwareRequest remoteClusterAware) {
                DiscoveryNode targetNode = remoteClusterAware.getPreferredTargetNode();
                transportService.sendRequest(
                    new ProxyConnection(connection, targetNode),
                    action.name(),
                    request,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(wrappedListener, action.getResponseReader())
                );
            } else {
                transportService.sendRequest(
                    connection,
                    action.name(),
                    request,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(wrappedListener, action.getResponseReader())
                );
            }
        }

        @Override
        public void close() {
            connection.close();
        }
    }
}
