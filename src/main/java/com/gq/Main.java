package com.gq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nasdaq.mme.common.soup.Connection;
import com.nasdaq.mme.common.soup.ConnectionListener;
import com.nasdaq.ouchitch.api.ItchMessageListener;
import com.nasdaq.ouchitch.itch.impl.ItchClient;
import com.nasdaq.ouchitch.itch.impl.ItchMessageFactorySet;
import com.nasdaq.ouchitch.utils.ClientException;
import com.nasdaq.ouchitch.utils.ConnectContextImpl;
import genium.trading.itch42.messages.Message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {
    private volatile long seq = 0L;
    private final Queue<ServerPair> servers = new PriorityBlockingQueue<>();
    private final Queue<SystemMessage> systemMessages = new PriorityBlockingQueue<>();
    private final ItchMessageFactorySet messageFactory = new ItchMessageFactorySet();

    private volatile ItchClient glimpseClient;
    private volatile Integer glimpseId;

    public static void main(String[] args) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Config config = mapper.readValue(Paths.get("config/application.json").toFile(), Config.class);
        System.out.println(config.failover.getRetry());
    }

    private PriorityBlockingQueue<ServerPair> toQueue(ServerPair sp, int retry) {
        return IntStream
                .range(0, retry)
                .mapToObj(n -> sp)
                .collect(Collectors.toCollection(PriorityBlockingQueue::new));
    }

    public Main(Config config) {
        ServerPair main = new ServerPair(config.glimpse.get(0), config.rt.get(0));
        ServerPair backup = new ServerPair(config.glimpse.get(1), config.rt.get(1));
        int retry = config.failover.getRetry();
        servers.addAll(toQueue(main, retry));
        servers.addAll(toQueue(backup, retry));
    }


    private ConnectContextImpl createContext(Config.Server config) {
        ConnectContextImpl connectContext = new ConnectContextImpl();
        connectContext.setUserName(config.getUser());
        connectContext.setPassword(config.getPassword());
        connectContext.setHost(config.getHost());
        connectContext.setPort(config.getPort());
        connectContext.setSequenceNumber(this.seq);
        return connectContext;
    }

    private ItchClient createItchClient() {
        return new ItchClient(new ConnectionListener() {
            @Override
            public void onDataReceived(Connection connection, ByteBuffer byteBuffer, long l) {

            }

            @Override
            public void onConnectionEstablished(Connection connection) {

            }

            @Override
            public void onConnectionClosed(Connection connection) {

            }

            @Override
            public void onConnectionRejected(Connection connection, char c) {

            }

            @Override
            public void onConnectionError(Connection connection, String s) {
                systemMessages.add(new SystemMessage.Restart());
            }

            @Override
            public void onHeartbeat(Connection connection) {

            }
        });
    }

    private ItchClient createGlimpseClient(ConnectContextImpl rtContext) {
        ItchClient itchClient = new com.nasdaq.ouchitch.itch.impl.ItchClient(new ConnectionListener() {
            @Override
            public void onDataReceived(Connection connection, ByteBuffer byteBuffer, long l) {
                seq = l;
                Message parsed = messageFactory.parse(byteBuffer);
                if (parsed.getMsgType() == 71) {
                    systemMessages.add(new SystemMessage.LogoffGlimpse());

                    ItchClient rtClient = createItchClient();
                    try {
                        rtClient.connect(rtContext);
                    } catch (ClientException e) {
                        systemMessages.add(new SystemMessage.Restart());
                    }
                }
            }

            @Override
            public void onConnectionEstablished(Connection connection) {

            }

            @Override
            public void onConnectionClosed(Connection connection) {

            }

            @Override
            public void onConnectionRejected(Connection connection, char c) {

            }

            @Override
            public void onConnectionError(Connection connection, String s) {
                systemMessages.add(new SystemMessage.Restart());
            }

            @Override
            public void onHeartbeat(Connection connection) {

            }
        });

        itchClient.registerMessageListener(new ItchMessageListener() {
            @Override
            public void onMessage(Message message, long l) {

            }
        });
        return itchClient;
    }

    public void start(){
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> {
            SystemMessage msg = systemMessages.poll();
            if (msg != null) {
                if (msg instanceof SystemMessage.Restart) {
                    ServerPair nextServer = servers.poll();
                    if (nextServer != null) {
                        run(nextServer);
                    } else {
                        System.exit(-20);
                    }
                } else if (msg instanceof SystemMessage.LogoffGlimpse && glimpseClient != null && glimpseId != null) {
                    try {
                        glimpseClient.logout(glimpseId);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    glimpseClient.close();
                }
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    private void run(ServerPair server) {
        ConnectContextImpl glimpseCtx = createContext(server.glimpse);
        ConnectContextImpl rtCtx = createContext(server.rt);
        glimpseClient =  createGlimpseClient(rtCtx);
        try {
            this.glimpseId = glimpseClient.connect(glimpseCtx);
        } catch (ClientException e) {
           systemMessages.add(new SystemMessage.Restart());
        }
    }

    public static class ServerPair {
        final Config.Server glimpse;
        final Config.Server rt;

        public ServerPair(Config.Server glimpse, Config.Server rt) {
            this.glimpse = glimpse;
            this.rt = rt;
        }
    }
}