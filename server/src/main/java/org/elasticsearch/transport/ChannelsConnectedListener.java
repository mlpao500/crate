package org.elasticsearch.transport;

import java.util.List;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.transport.TcpTransport.NodeChannels;
import org.elasticsearch.transport.Transport.Connection;


public final class ChannelsConnectedListener implements ActionListener<Void> {

    private final TcpTransport tcpTransport;
    private final DiscoveryNode node;
    private final ConnectionProfile connectionProfile;
    private final List<TcpChannel> channels;
    private final ActionListener<Transport.Connection> listener;
    private final CountDown countDown;

    public ChannelsConnectedListener(TcpTransport tcpTransport,
                                     DiscoveryNode node,
                                     ConnectionProfile connectionProfile,
                                     List<TcpChannel> channels,
                                     ActionListener<Transport.Connection> listener) {
        this.tcpTransport = tcpTransport;
        this.node = node;
        this.connectionProfile = connectionProfile;
        this.channels = channels;
        this.listener = listener;
        this.countDown = new CountDown(channels.size());
    }

    @Override
    public void onResponse(Void v) {
        // Returns true if all connections have completed successfully
        if (countDown.countDown()) {
            final TcpChannel handshakeChannel = channels.get(0);
            try {
                this.tcpTransport.executeHandshake(
                    node,
                    handshakeChannel,
                    connectionProfile,
                    ActionListener.wrap(
                        version -> {
                            NodeChannels nodeChannels = this.tcpTransport.new NodeChannels(node, channels, connectionProfile, version);
                            long relativeMillisTime = this.tcpTransport.threadPool.relativeTimeInMillis();
                            nodeChannels.channels.forEach(ch -> {
                                // Mark the channel init time
                                ch.getChannelStats().markAccessed(relativeMillisTime);
                                ch.addCloseListener(ActionListener.wrap(nodeChannels::close));
                            });
                            this.tcpTransport.keepAlive.registerNodeConnection(nodeChannels.channels, connectionProfile);
                            listener.onResponse(nodeChannels);
                        },
                        e -> {
                            closeAndFail(e instanceof ConnectTransportException ? e : new ConnectTransportException(node, "general node connection failure", e));
                        }
                    )
                );
            } catch (Exception ex) {
                closeAndFail(ex);
            }
        }
    }

    @Override
    public void onFailure(Exception ex) {
        if (countDown.fastForward()) {
            closeAndFail(new ConnectTransportException(node, "connect_exception", ex));
        }
    }

    public void onTimeout() {
        if (countDown.fastForward()) {
            closeAndFail(new ConnectTransportException(node, "connect_timeout[" + connectionProfile.getConnectTimeout() + "]"));
        }
    }

    private void closeAndFail(Exception e) {
        try {
            CloseableChannel.closeChannels(channels, false);
        } catch (Exception ex) {
            e.addSuppressed(ex);
        } finally {
            listener.onFailure(e);
        }
    }
}
