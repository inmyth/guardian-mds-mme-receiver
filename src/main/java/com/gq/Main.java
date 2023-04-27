package com.gq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nasdaq.mme.common.soup.Connection;
import com.nasdaq.mme.common.soup.ConnectionListener;
import com.nasdaq.ouchitch.itch.impl.ItchClient;
import com.nasdaq.ouchitch.itch.impl.ItchMessageFactorySet;
import com.nasdaq.ouchitch.utils.ClientException;
import com.nasdaq.ouchitch.utils.ConnectContextImpl;
import genium.trading.itch42.messages.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {
    private volatile long seq = 0L;
    private final Queue<ServerPair> servers = new LinkedBlockingQueue<>();
    private final Queue<SystemMessage> systemMessages = new LinkedBlockingQueue<>();
    private final ItchMessageFactorySet messageFactory = new ItchMessageFactorySet();
    private final BlockingQueue<RawMessage> rawMessages = new LinkedBlockingQueue<>();
    private volatile ItchClient glimpseClient;
    private volatile Integer glimpseId;
    private final KafkaProducer<String, byte[]> producer;
    private final List<String> topics;
    private final String alertTopic;
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Config config = mapper.readValue(Paths.get("config/application.json").toFile(), Config.class);
        Properties kafkaProps = new Properties();
        try (InputStream inputStream = new FileInputStream("config/kafka.properties")) {
            kafkaProps.load(inputStream);
        }
        Main main = new Main(config, kafkaProps);
        try {
            main.start();
        } catch (Exception e) {
            System.out.println("Main.main() try-catch activated, shutting down");
            System.exit(1);
            throw e;
        }
    }

    private LinkedBlockingQueue<ServerPair> toQueue(ServerPair sp, int retry) {
        return IntStream
                .range(0, retry)
                .mapToObj(n -> sp)
                .collect(Collectors.toCollection(LinkedBlockingQueue::new));
    }

    public Main(Config config, Properties kafkaProps) {
        logger.info("main");
        ServerPair main = new ServerPair(config.glimpse.get(0), config.rt.get(0));
        ServerPair backup = new ServerPair(config.glimpse.get(1), config.rt.get(1));
        producer = new KafkaProducer<>(kafkaProps);
        topics = config.topics;
        alertTopic = config.alertTopic;
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
                Message parsed = messageFactory.parse(byteBuffer);
                if (parsed != null) {
                    logMessage(parsed, l);
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

    private void logMessage(Message msg, Long seq) {
        if (msg instanceof OrderBookDirectoryMessageSetImpl) {
            OrderBookDirectoryMessageSetImpl a = (OrderBookDirectoryMessageSetImpl) msg;
             logger.info("{} OrderBookDirectoryMessageSetImpl symbol:{}, orderbookId:{}, longName:{}, isin:{}, financialProduct:{}, " +
                             "tradingCurrency:{}, decimalsInPrice:{}, decimalsInNominalValue:{}, roundLotSize:{}, " +
                             "nominalValue:{}, numberOfLegs:{}, underlyingName:{}, underlying:{}, underlyingOrderBookId:{}," +
                             "expirationDate:{}, decimalsInStrikePrice:{}, optionType:{}, exchangeCode:{}, marketCode:{}," +
                             "priceQuotationFactor:{}, notificationSign:{}, otherSign:{}, allowNvdr:{}, allowShortSell:{}," +
                             "allowShortSellOnNvdr:{}, allowTtf:{}, parValue:{}, firstTradingDate:{}, firstTradingTime:{}" +
                             "lastTradingDate:{}, lastTradingTime:{}, marketSegment:{}, physicalDelivery:{}" +
                             " contractSize:{}, sectorCode:{}, originatesFrom:{}, modifier:{}, notationDate:{}, decimalsInContractSizePQF:{}" +
                             " strikePrice:{}, corporateActionCode:{}, decimalsInPrice:{}, roundLotSize:{}, status:{}, exchangeCode:{}",
                     seq,
                     new String(a.getSymbol()),
                     a.getOrderBookId(),
                     new String(a.getLongName()),
                     new String(a.getIsin()),
                     new String(a.getFinancialProduct()),
                     new String(a.getTradingCurrency()),
                     a.getDecimalsInPrice(),
                     a.getDecimalsInNominalValue(),
                     a.getRoundLotSize(),
                     a.getNominalValue(),
                     a.getNumberOfLegs(),
                     new String(a.getUnderlyingName()),
                     a.getUnderlying(),
                     a.getUnderlyingOrderBookId(),
                     a.getExpirationDate(),
                     a.getDecimalsInStrikePrice(),
                     a.getOptionType(),
                     (char)(a.getExchangeCode()),
                     a.getMarketCode(),
                     a.getPriceQuotationFactor(),
                     new String(a.getNotificationSign()),
                     new String(a.getOtherSign()),
                     (char)(a.getAllowNvdr() & 0xFF),
                     (char)(a.getAllowShortSell() & 0xFF),
                     (char)(a.getAllowShortSellOnNvdr() & 0xFF),
                     (char)(a.getAllowTtf() & 0xFF),
                     a.getParValue(),
                     a.getFirstTradingDate(),
                     a.getFirstTradingTime(),
                     a.getLastTradingDate(),
                     a.getLastTradingTime(),
                     new String(a.getMarketSegment()),
                     (char)(a.getPhysicalDelivery() & 0xFF),
                     a.getContractSize(),
                     new String(a.getSectorCode()),
                     new String(a.getOriginatesFrom()),
                     a.getModifier(),
                     a.getNotationDate(),
                     a.getDecimalsInContractSizePQF(),
                     a.getStrikePrice(),
                     new String(a.getCorporateActionCode()),
                     a.getDecimalsInPrice(),
                     a.getRoundLotSize(),
                     (char)(a.getStatus() & 0xFF),
                     (char)(a.getExchangeCode() & 0xFF));
        } else if (msg instanceof ExchangeDirectoryMessage) {
            ExchangeDirectoryMessage a = (ExchangeDirectoryMessage) msg;
            logger.info("{} ExchangeDirectoryMessage exchangeCode:{}, exchangeName:{}",
                    seq,
                    a.getExchangeCode(),
                    new String(a.getExchangeName())
                    );
        } else if (msg instanceof MarketDirectoryMessageSet) {
            MarketDirectoryMessageSet a = (MarketDirectoryMessageSet) msg;
            logger.info("{} MarketDirectoryMessageSet marketCode:{}, marketName:{}, marketDesc:{}"
                    ,seq, a.getMarketCode(), new String(a.getMarketName()), new String(a.getMarketDescription()));
        } else if (msg instanceof CombinationOrderBookLegMessage) {
            CombinationOrderBookLegMessage a = (CombinationOrderBookLegMessage) msg;
            logger.info("{} CombinationOrderBookLegMessage oid:{}, legOid:{}, legRatio:{}, legSide:{} "
                    ,seq, a.getOrderBookId(), a.getLegOrderBookId(), a.getLegRatio(), (char)(a.getLegSide() & 0xFF));
        } else if (msg instanceof TickSizeTableMessage) {
            TickSizeTableMessage a = (TickSizeTableMessage) msg;
            logger.info("{} TickSizeTableMessage oid:{}, priceTo:{}, priceFrom:{}", seq, a.getOrderBookId(), a.getPriceTo(), a.getPriceFrom());
        } else if (msg instanceof PriceLimitMessage) {
            PriceLimitMessage a = (PriceLimitMessage) msg;
            logger.info("{} PriceLimitMessage oid:{}, lowerLimit:{}, upperLimit:{}", seq, a.getOrderBookId(), a.getLowerLimit(), a.getUpperLimit());
        } else if (msg instanceof SystemEventMessage) {
            SystemEventMessage a = (SystemEventMessage) msg;
            logger.info("{} SystemEventMessage event:{}", seq, a.getEvent());
        } else if (msg instanceof OrderBookStateMessage) {
            OrderBookStateMessage a = (OrderBookStateMessage) msg;
            logger.info("{} OrderBookStateMessage oid:{}, stateName:{}", seq, a.getOrderBookId(), new String(a.getStateName()));
        } else if (msg instanceof  HaltInformationMessage) {
            HaltInformationMessage a = (HaltInformationMessage) msg;
            logger.info("{} HaltInformationMessage oid:{}, instrumentState:{}", seq, a.getOrderBookId(), new String(a.getInstrumentState()));
        } else if (msg instanceof MarketByPriceMessage) {
            MarketByPriceMessage a = (MarketByPriceMessage) msg;
            String items  = a.getItems().stream().map(p -> "levelUpdateAction:" + (char) (p.getLevelUpdateAction() & 0xFF) +
                        ", level:" + p.getLevel() +
                        ", side:" + (char) (p.getSide() & 0xFF) +
                        ", price:" + p.getPrice() +
                        ", qty:" + p.getQuantity() +
                        ", numberOfDeletes:" + p.getNumberOfDeletes()).collect(Collectors.joining("\n"));
            logger.info("{} MarketByPriceMessage oid:{}, maxLevel:{}\n{}", seq, a.getOrderBookId(), a.getMaximumLevel(), items);
        } else if (msg instanceof EquilibriumPriceMessage) {
            EquilibriumPriceMessage a = (EquilibriumPriceMessage) msg;
            logger.info("{} EquilibriumPriceMessage oid:{}, price:{}, bestAskPrice:{}, askQty:{}, bestAskQty:{}, " +
                            "bestBidPrice:{}, bidQty:{}, bestBidQty:{}", seq, a.getOrderBookId(),
                    a.getPrice(),a.getBestAskPrice(), a.getAskQuantity(), a.getBestAskQuantity(),
                    a.getBestBidPrice(), a.getBidQuantity(), a.getBestBidQuantity()
                    );
        } else if (msg instanceof TradeTickerMessageSet) {
            TradeTickerMessageSet a = (TradeTickerMessageSet) msg;
            logger.info("{} TradeTickerMessageSet oid:{}, dealId:{}, dealSource:{}, price:{}, qty:{}, dealTime:{}, action:{}" +
                            " aggressor:{}, tradeReportCode:{}",
                    seq, a.getOrderBookId(), a.getDealId(), (char) (a.getDealSource() & 0xFF), a.getPrice(), a.getQuantity(),
                    a.getDealDateTime(), (char) (a.getAction() & 0xFF), (char) (a.getAggressor() & 0xFF), a.getTradeReportCode()
                    );
        } else if (msg instanceof TradeStatisticsMessage) {
            TradeStatisticsMessage a = (TradeStatisticsMessage) msg;
            logger.info("{} TradeStatisticsMessage oid:{}, openPrice:{}, highPrice:{}, lowPrice:{}, lastPrice:{}, lastAuctionPrice:{}, " +
                            "lastQty:{}, turnOverQty:{}, reportedTurnOverQty:{}, turnOverValue:{}",
                    seq, a.getOrderBookId(), a.getOpenPrice(), a.getHighPrice(), a.getLowPrice(),
                    a.getLastPrice(), a.getLastAuctionPrice(), a.getLastQuantity(), a.getTurnOverQuantity(), a.getReportedTurnOverQuantity(),
                    a.getTurnOverValue()
            );
        } else if (msg instanceof TradeStatisticsMessageSet) {
            TradeStatisticsMessageSet a  = (TradeStatisticsMessageSet) msg;
            logger.info("{} TradeStatisticsMessage oid:{}, openPrice:{}, highPrice:{}, lowPrice:{}, lastPrice:{}, lastAuctionPrice:{}, " +
                            "turnOverQty:{}, reportedTurnOverQty:{}, turnOverValue:{}, reportedTurnOverValue:{}, " +
                            "avgPrice:{}, totalNumberOfTrades:{}",
                    seq, a.getOrderBookId(), a.getOpenPrice(), a.getHighPrice(), a.getLowPrice(),
                    a.getLastPrice(), a.getLastAuctionPrice(), a.getTurnOverQuantity(), a.getReportedTurnOverQuantity(),
                    a.getTurnOverValue(), a.getReportedTurnOverValue(), a.getAveragePrice(), a.getTotalNumberOfTrades()
            );
        } else if (msg instanceof InavMessage) {
            InavMessage a = (InavMessage) msg;
            logger.info("{} InavMessage oid:{}, inav:{}, change:{}, percentageChange:{}, timestamp:{}",
                    seq, a.getOrderBookId(), a.getInav(), a.getChange(), a.getPercentageChange(), a.getTimestamp());
        } else if (msg instanceof IndexPriceMessageSet) {
            IndexPriceMessageSet a = (IndexPriceMessageSet) msg;
            logger.info("{} IndexPriceMessageSet oid:{}, value:{}, highValue:{}, lowValue:{}, openValue:{}, " +
                            "tradedVol:{}, tradedValue:{}, change:{}, changePercent:{}, previousClose:{}, close:{}, " +
                            "ts:{}",
                    seq, a.getOrderBookId(), a.getValue(), a.getHighValue(), a.getLowValue(), a.getOpenValue(),
                    a.getTradedVolume(), a.getTradedValue(), a.getChange(), a.getChangePercent(), a.getPreviousClose(),
                    a.getClose(), a.getTimestamp()
                    );
        } else if (msg instanceof MarketStatisticsMessage) {
            MarketStatisticsMessage a = (MarketStatisticsMessage) msg;
            logger.info("{} MarketStatisticsMessage marketStatId:{}, currency:{}, marketStatTime:{}, " +
                            "totalTrades:{}, totalQty:{}, totalValue:{}, upQty:{}, downQty:{}, noChangeQty:{}, " +
                            "upShares:{}, downShares:{}, noChangeShares:{}",
                    seq, new String(a.getMarketStatisticsId()), new String(a.getCurrency()), a.getMarketStatisticsTime(),
                    a.getTotalTrades(), a.getTotalQuantity(), a.getTotalValue(), a.getUpQuantity(), a.getDownQuantity(),
                    a.getNoChangeQuantity(), a.getUpShares(), a.getDownShares(), a.getNoChangeShares()
                    );
        } else if (msg instanceof ReferencePriceMessage) {
            ReferencePriceMessage a = (ReferencePriceMessage) msg;
            logger.info("{} ReferencePriceMessage oid:{}, price:{}, priceType:{}, updatedTs:{}",
                    seq, a.getOrderBookId(), a.getPrice(), (char)(a.getPriceType() & 0xFF), a.getUpdatedTimestamp());
        } else if (msg instanceof OpenInterestMessage){
            OpenInterestMessage a = (OpenInterestMessage) msg;
            logger.info("{} OpenInterestMessage oid:{}, openInterest:{}, ts:{}",
                    seq, a.getOrderBookId(), a.getOpenInterest(), a.getTimestamp());
        } else if (msg instanceof MarketAnnouncementMessage) {
            MarketAnnouncementMessage a = (MarketAnnouncementMessage) msg;
            logger.info("{} MarketAnnouncementMessage oid:{}, header:{}",
                    seq, a.getOrderBookId(), new String(a.getHeader()));
        } else if (msg instanceof GlimpseSnapshotMessage) {
            GlimpseSnapshotMessage a = (GlimpseSnapshotMessage) msg;
            logger.info("{} GlimpseSnapshotMessage itchMessageSeq: {}", seq, new String(a.getItchSequenceNumber()));
        }
        else {
             if (msg.getMsgType() != 84) {
                logger.info("{} UNMATCHED_MESSAGE type:{}, class:{}", seq, (char) (msg.getMsgType() & 0xFF), msg.getClass());
            }
        }
    }

    private ItchClient createGlimpseClient(ConnectContextImpl rtContext) {
        ItchClient itchClient = new com.nasdaq.ouchitch.itch.impl.ItchClient(new ConnectionListener() {
            @Override
            public void onDataReceived(Connection connection, ByteBuffer byteBuffer, long l) {
                seq = l;
                Message parsed = messageFactory.parse(byteBuffer);
                if (parsed != null) {
                    logMessage(parsed, l);
                    byte[] content = new byte[byteBuffer.remaining()];
                    byteBuffer.get(content);
                    rawMessages.add(new RawMessage(l, parsed.getMsgType(), content));
                    if (parsed.getMsgType() == 71) {
                        GlimpseSnapshotMessage m = (GlimpseSnapshotMessage) parsed;
                        seq = Long.parseLong(new String(m.getItchSequenceNumber()));
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

    public void start() throws Exception {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

        ScheduledFuture<?> scheduledFuture = executor.scheduleAtFixedRate(() -> {
                SystemMessage msg = systemMessages.poll();
                if (msg != null) {
                    if (msg instanceof SystemMessage.Restart) {
                        ServerPair nextServer = servers.poll();
                        if (nextServer != null) {
                            logger.info("Starting Glimpse connection");
                            producer.send(new ProducerRecord<>(alertTopic, Long.toString(System.currentTimeMillis()),
                                ("Starting Receiver from Glimpse at " + new Date()).getBytes(StandardCharsets.UTF_8)),
                                (event, ex) -> {
                                    if (ex != null) {
                                        logger.error("Cannot send to alert: " + ex.getMessage());
                                        throw new RuntimeException(ex.getMessage());
                                    }
                                });
                            run(nextServer);
                        } else {
                            throw new RuntimeException("Restart cannot find Glimpse server from config");
                        }
                    } else if (msg instanceof SystemMessage.RestartRt) {
                        ServerPair nextServer = servers.poll();
                        if (nextServer != null) {
                            logger.info("Starting RT connection");
                            ConnectContextImpl rtCtx = createContext(nextServer.rt, seq + 1);
                            runRt(rtCtx);
                        } else {
                            throw new RuntimeException("RestartRT cannot find RT server from config");
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
                    for (String t: topics) {
                        producer.send(new ProducerRecord<>(t, Long.toString(rawMessage.sequenceNumber), rawMessage.content), (event, ex) -> {
                            if (ex != null) {
                                logger.error(ex.getMessage());
                                throw new RuntimeException(ex.getMessage());
                            }
                            else
                                System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", t, rawMessage.sequenceNumber, rawMessage.msgType);
                        });
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        systemMessages.add(new SystemMessage.Restart());
        try {
            scheduledFuture.get();
        } catch (Exception e) {
            producer.send(new ProducerRecord<>(alertTopic, Long.toString(System.currentTimeMillis()),
                ("Seq: " + seq + " " +  e.getMessage()).getBytes(StandardCharsets.UTF_8)),
                (event, ex) -> {
                    if (ex != null) {
                        logger.error("Cannot send to alert: " + ex.getMessage());
                        throw new RuntimeException(e.getMessage());
                    }
                });
            throw new Exception(e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    private void run(ServerPair server) {
        ConnectContextImpl glimpseCtx = createContext(server.glimpse, seq + 1);
        ConnectContextImpl rtCtx = createContext(server.rt, -100); // seq is not needed here
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