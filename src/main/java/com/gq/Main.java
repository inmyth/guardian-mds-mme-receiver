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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {
    private volatile long seq = 1L;
    private final Queue<ServerPair> servers = new LinkedBlockingQueue<>();
    private final Queue<SystemMessage> systemMessages = new LinkedBlockingQueue<>();
    private final ItchMessageFactorySet messageFactory = new ItchMessageFactorySet();
    private final BlockingQueue<RawMessage> rawMessages = new LinkedBlockingQueue<>();
    private volatile ItchClient glimpseClient;
    private volatile Integer glimpseId;
    private final KafkaProducer<String, byte[]> producer;
    private final String kafkaTopic;
    Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        System.setProperty("logback.configurationFile", "config/logback.xml");
        Config config = mapper.readValue(Paths.get("config/application.json").toFile(), Config.class);
        Properties kafkaProps = new Properties();
        try (InputStream inputStream = new FileInputStream("config/kafka.properties")) {
            kafkaProps.load(inputStream);
        }
        Main main = new Main(config, kafkaProps);
        main.start();
    }

    private LinkedBlockingQueue<ServerPair> toQueue(ServerPair sp, int retry) {
        return IntStream
                .range(0, retry)
                .mapToObj(n -> sp)
                .collect(Collectors.toCollection(LinkedBlockingQueue::new));
    }

    public Main(Config config, Properties kafkaProps) {
        ServerPair main = new ServerPair(config.glimpse.get(0), config.rt.get(0));
        ServerPair backup = new ServerPair(config.glimpse.get(1), config.rt.get(1));
        producer = new KafkaProducer<>(kafkaProps);
        kafkaTopic = config.getKafkaTopic();
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
                if (parsed != null) {
                    logger.info("Itch "+ timestamp + "  type = " + parsed.getMsgType() +
                             " " + parsed.getClass() +
                             " seq=" + l);
                    byte[] content = new byte[byteBuffer.remaining()];
                    byteBuffer.get(content);
                    rawMessages.add(new RawMessage(l, parsed.getMsgType(), content));
                }
            }

            @Override
            public void onConnectionEstablished(Connection connection) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                logger.info("Itch "+ timestamp + " connection established");
            }

            @Override
            public void onConnectionClosed(Connection connection) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                logger.info("Itch "+ timestamp + " connection closed");
            }

            @Override
            public void onConnectionRejected(Connection connection, char c) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                logger.info("Itch "+ timestamp + " connection rejected " + c);
            }

            @Override
            public void onConnectionError(Connection connection, String s) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                logger.info("Itch "+ timestamp + " connection error, " + s +" restarting");
                systemMessages.add(new SystemMessage.RestartRt());
            }

            @Override
            public void onHeartbeat(Connection connection) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                logger.info("Itch "+ timestamp + " connection heartbeat");
            }
        });

        itchClient.registerMessageListener((message, l) -> {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            logger.info("RT onMessage" + timestamp + " " + message.getMsgType());
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
                if (parsed != null) {
                    logger.info("Glimpse "+ timestamp + "  type = " + parsed.getMsgType() +
                             " " + parsed.getClass() +
                             " seq=" + l);
                    byte[] content = new byte[byteBuffer.remaining()];
                    byteBuffer.get(content);
                    rawMessages.add(new RawMessage(l, parsed.getMsgType(), content));
                    if (parsed.getMsgType() == 71) {
                        systemMessages.add(new SystemMessage.LogoffGlimpse());
                        runRt(rtContext);
                    }
                }
            }

            @Override
            public void onConnectionEstablished(Connection connection) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                logger.info("Glimpse "+ timestamp + " connection established");
            }

            @Override
            public void onConnectionClosed(Connection connection) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                logger.info("Glimpse "+ timestamp + " connection closed");
            }

            @Override
            public void onConnectionRejected(Connection connection, char c) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                logger.info("Glimpse "+ timestamp + " connection rejected " + c);
            }

            @Override
            public void onConnectionError(Connection connection, String s) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                logger.info("Glimpse "+ timestamp + " connection error " + s);
                systemMessages.add(new SystemMessage.Restart());
            }

            @Override
            public void onHeartbeat(Connection connection) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                logger.info("Glimpse "+ timestamp + " connection heartbeat");
            }
        });

        itchClient.registerMessageListener((message, l) -> {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            logger.info("Glimpse Message" + timestamp + " " + message.getMsgType());
        });
        return itchClient;
    }

    public void start() {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        executor.scheduleAtFixedRate(() -> {
            SystemMessage msg = systemMessages.poll();

            if (msg != null) {
                if (msg instanceof SystemMessage.Restart) {
                    ServerPair nextServer = servers.poll();
                    if (nextServer != null) {
                        logger.info("Starting Glimpse connection");
                        run(nextServer);
                    } else {
                        System.exit(-20);
                    }
                } else if (msg instanceof SystemMessage.RestartRt) {
                    ServerPair nextServer = servers.poll();
                    if (nextServer != null) {
                        logger.info("Starting RT connection");
                        ConnectContextImpl rtCtx = createContext(nextServer.rt, seq);
                        runRt(rtCtx);
                    } else {
                        System.exit(-30);
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
        executor.execute(() -> {
            while (true) {
                try {
                    RawMessage rawMessage = rawMessages.take();
                    producer.send(new ProducerRecord<>(kafkaTopic, Long.toString(rawMessage.sequenceNumber), rawMessage.content), (event, ex) -> {
                        if (ex != null)
                            ex.printStackTrace();
                        else
                            System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", kafkaTopic, rawMessage.sequenceNumber, rawMessage.msgType);
                    });
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        systemMessages.add(new SystemMessage.Restart());
    }

    private void run(ServerPair server) {
        ConnectContextImpl glimpseCtx = createContext(server.glimpse, seq);
        ConnectContextImpl rtCtx = createContext(server.rt, seq);
        glimpseClient =  createGlimpseClient(rtCtx);
        try {
            this.glimpseId = glimpseClient.connect(glimpseCtx);
        } catch (ClientException e) {
            e.printStackTrace();
            systemMessages.add(new SystemMessage.Restart());
        }
    }

    private void runRt(ConnectContextImpl rtCtx) {
        ItchClient rtClient = createRtClient();
        try {
            logger.info("Connecting to RT");
            rtCtx.setSequenceNumber(seq);
            rtClient.connect(rtCtx);
        } catch (ClientException e) {
            e.printStackTrace();
            systemMessages.add(new SystemMessage.RestartRt());
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

    public static class RawMessage{
        public byte[] content;
        public long sequenceNumber;
        public byte msgType;

        public RawMessage(long sequenceNumber, byte msgType, byte[] content){
            this.sequenceNumber = sequenceNumber;
            this.msgType = msgType;
            this.content = content;
        }
    }
}