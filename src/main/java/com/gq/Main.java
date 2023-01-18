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
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {
    private volatile long seq = 1L;
    private final Queue<ServerPair> servers = new LinkedBlockingQueue<>();
    private final Queue<SystemMessage> systemMessages = new LinkedBlockingQueue<>();
    private final ItchMessageFactorySet messageFactory = new ItchMessageFactorySet();

    private volatile ItchClient glimpseClient;
    private volatile Integer glimpseId;

    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy MM dd HH:mm:ss");


    public static void main(String[] args) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Config config = mapper.readValue(Paths.get("config/application.json").toFile(), Config.class);
        Main main = new Main(config);
        main.start();
    }

    private LinkedBlockingQueue<ServerPair> toQueue(ServerPair sp, int retry) {
        return IntStream
                .range(0, retry)
                .mapToObj(n -> sp)
                .collect(Collectors.toCollection(LinkedBlockingQueue::new));
    }

    public Main(Config config) {
        ServerPair main = new ServerPair(config.glimpse.get(0), config.rt.get(0));
        ServerPair backup = new ServerPair(config.glimpse.get(1), config.rt.get(1));
        int retry = config.failover.getRetry();
        servers.addAll(toQueue(main, retry));
        servers.addAll(toQueue(backup, retry));
    }


    private ConnectContextImpl createContext(Config.Server config, long seq) {
        ConnectContextImpl connectContext = new ConnectContextImpl();
        connectContext.setUserName(config.getUser());
        connectContext.setPassword(config.getPassword());
        connectContext.setHost(config.getHost());
        connectContext.setPort(config.getPort());
        connectContext.setSequenceNumber(seq);
        return connectContext;
    }

    private ItchClient createRtClient() {
        ItchClient itchClient = new ItchClient(new ConnectionListener() {
            @Override
            public void onDataReceived(Connection connection, ByteBuffer byteBuffer, long l) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                Message parsed = messageFactory.parse(byteBuffer);
                System.out.println("Itch "+ timestamp + "  type = " + parsed.getMsgType() +
                        " " + parsed.getClass() +
                        " seq=" + l);
            }

            @Override
            public void onConnectionEstablished(Connection connection) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                System.out.println("Itch "+ timestamp + " connection established");
            }

            @Override
            public void onConnectionClosed(Connection connection) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                System.out.println("Itch "+ timestamp + " connection closed");

            }

            @Override
            public void onConnectionRejected(Connection connection, char c) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                System.out.println("Itch "+ timestamp + " connection rejected " + c);
            }

            @Override
            public void onConnectionError(Connection connection, String s) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                System.out.println("Itch "+ timestamp + " connection error, " + s +" restarting");
                systemMessages.add(new SystemMessage.Restart());
            }

            @Override
            public void onHeartbeat(Connection connection) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                System.out.println("Itch "+ timestamp + " connection heartbeat");
            }
        });

        itchClient.registerMessageListener(new ItchMessageListener() {
            @Override
            public void onMessage(Message message, long l) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                System.out.println("RT onMessage" + timestamp + " " + message.getMsgType());
            }
        });
        return itchClient;
    }

    private ItchClient createGlimpseClient(ConnectContextImpl rtContext) {
        ItchClient itchClient = new com.nasdaq.ouchitch.itch.impl.ItchClient(new ConnectionListener() {
            @Override
            public void onDataReceived(Connection connection, ByteBuffer byteBuffer, long l) {
                seq = l;
                Message parsed = messageFactory.parse(byteBuffer);
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                System.out.println("Glimpse "+ timestamp + "  type = " + parsed.getMsgType() +
                        " " + parsed.getClass() +
                        " seq=" + l);
                if (parsed.getMsgType() == 71) {
                    systemMessages.add(new SystemMessage.LogoffGlimpse());
                    ItchClient rtClient = createRtClient();
                    try {
                        System.out.println("Connecting to RT");
                        rtContext.setSequenceNumber(seq);
                        rtClient.connect(rtContext);
                    } catch (ClientException e) {
                        System.out.println(e);
                        systemMessages.add(new SystemMessage.Restart());
                    }
                }
            }

            @Override
            public void onConnectionEstablished(Connection connection) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                System.out.println("Glimpse "+ timestamp + " connection established");
            }

            @Override
            public void onConnectionClosed(Connection connection) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                System.out.println("Glimpse "+ timestamp + " connection closed");
            }

            @Override
            public void onConnectionRejected(Connection connection, char c) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                System.out.println("Glimpse "+ timestamp + " connection rejected " + c);
            }

            @Override
            public void onConnectionError(Connection connection, String s) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                System.out.println("Glimpse "+ timestamp + " connection error " + s);
                systemMessages.add(new SystemMessage.Restart());
            }

            @Override
            public void onHeartbeat(Connection connection) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                System.out.println("Glimpse "+ timestamp + " connection heartbeat");
            }
        });

        itchClient.registerMessageListener(new ItchMessageListener() {
            @Override
            public void onMessage(Message message, long l) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                System.out.println("Glimpse Message" + timestamp + " " + message.getMsgType());
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
                        System.out.println("Starting connection");
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
        systemMessages.add(new SystemMessage.Restart());
    }

    private void run(ServerPair server) {
        ConnectContextImpl glimpseCtx = createContext(server.glimpse, seq);
        ConnectContextImpl rtCtx = createContext(server.rt, seq);
        glimpseClient =  createGlimpseClient(rtCtx);
        try {
            this.glimpseId = glimpseClient.connect(glimpseCtx);
        } catch (ClientException e) {
            System.out.println(e);
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