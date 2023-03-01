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
import java.nio.file.Paths;
import java.sql.Timestamp;
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
    private final String redisTopic;
    private final String mysqlTopic;
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
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
        logger.info("main");
        ServerPair main = new ServerPair(config.glimpse.get(0), config.rt.get(0));
        ServerPair backup = new ServerPair(config.glimpse.get(1), config.rt.get(1));
        producer = new KafkaProducer<>(kafkaProps);
        redisTopic = config.getRedisTopic();
        mysqlTopic = config.getMysqlTopic();
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
//                    logger.info("Itch "+ timestamp + "  type = " + parsed.getMsgType() +
//                             " " + parsed.getClass() +
//                             " seq=" + l);
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
/*
    protected final byte[] symbol = new byte[32];
    protected final byte[] longName = new byte[32];
    protected final byte[] isin = new byte[12];
    protected final byte[] financialProduct = new byte[3];
    protected final byte[] tradingCurrency = new byte[3];
    protected short decimalsInPrice;
    protected short decimalsInNominalValue;
    protected int roundLotSize;
    protected long nominalValue;
    protected byte numberOfLegs;
    protected final byte[] underlyingName = new byte[6];
    protected int underlying;
    protected int underlyingOrderBookId;
    protected int strikePrice;
    protected int expirationDate;
    protected short decimalsInStrikePrice;
    protected byte optionType;
    protected byte exchangeCode;
    protected byte marketCode;
    protected long priceQuotationFactor;
    protected final byte[] corporateActionCode = new byte[32];
    protected final byte[] notificationSign = new byte[32];
    protected final byte[] otherSign = new byte[32];
    protected byte allowNvdr;
    protected byte allowShortSell;
    protected byte allowShortSellOnNvdr;
    protected byte allowTtf;
    protected long parValue;
    protected int firstTradingDate;
    protected int firstTradingTime;
    protected int lastTradingDate;
    protected int lastTradingTime;
    protected final byte[] marketSegment = new byte[4];
    protected byte physicalDelivery;
    protected int contractSize;
    protected final byte[] sectorCode = new byte[4];
    protected final byte[] originatesFrom = new byte[32];
    protected byte status;
    protected short modifier;
    protected int notationDate;
    protected short decimalsInContractSizePQF;
 */
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
                     a.getExchangeCode(),
                     a.getMarketCode(),
                     a.getPriceQuotationFactor(),
                     new String(a.getNotificationSign()),
                     new String(a.getOtherSign()),
                     a.getAllowNvdr(),
                     a.getAllowShortSell(),
                     a.getAllowShortSellOnNvdr(),
                     a.getAllowTtf(),
                     a.getParValue(),
                     a.getFirstTradingDate(),
                     a.getFirstTradingTime(),
                     a.getLastTradingDate(),
                     a.getLastTradingTime(),
                     new String(a.getMarketSegment()),
                     a.getPhysicalDelivery(),
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
                     a.getStatus(),
                     a.getExchangeCode());
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
                    ,seq, a.getOrderBookId(), a.getLegOrderBookId(), a.getLegRatio(), a.getLegSide());
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
            String items  = a.getItems().stream().map(p -> {
                return "levelUpdateAction:" + p.getLevelUpdateAction() +
                        ", side:" + p.getSide() +
                        ", price:" + p.getPrice() +
                        ", qty:" + p.getQuantity() +
                        ", numberOfDeletes:" + p.getNumberOfDeletes();
            }).collect(Collectors.joining("\n"));
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
                    seq, a.getOrderBookId(), a.getDealId(), a.getDealSource(), a.getPrice(), a.getQuantity(),
                    a.getDealDateTime(), a.getAction(), a.getAggressor(), a.getTradeReportCode()
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
                    seq, new String(a.getMarketStatisticsId()), a.getCurrency(), a.getMarketStatisticsTime(),
                    a.getTotalTrades(), a.getTotalQuantity(), a.getTotalValue(), a.getUpQuantity(), a.getDownQuantity(),
                    a.getNoChangeQuantity(), a.getUpShares(), a.getDownShares(), a.getNoChangeShares()
                    );
        } else if (msg instanceof ReferencePriceMessage) {
            ReferencePriceMessage a = (ReferencePriceMessage) msg;
            logger.info("{} ReferencePriceMessage oid:{}, price:{}, priceType:{}, updatedTs:{}",
                    seq, a.getOrderBookId(), a.getPrice(), a.getPriceType(), a.getUpdatedTimestamp());
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
                logger.info("{} UNMATCHED_MESSAGE type:{}, class:{}", seq, msg.getMsgType(), msg.getClass());
            }
        }
    }

    private ItchClient createGlimpseClient(ConnectContextImpl rtContext) {
        ItchClient itchClient = new com.nasdaq.ouchitch.itch.impl.ItchClient(new ConnectionListener() {
            @Override
            public void onDataReceived(Connection connection, ByteBuffer byteBuffer, long l) {
                seq = l;
                Message parsed = messageFactory.parse(byteBuffer);
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                if (parsed != null) {
//                    logger.info("Glimpse "+ timestamp + "  type = " + parsed.getMsgType() +
//                             " " + parsed.getClass() +
//                             " seq=" + l);
                    logMessage(parsed, l);
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
                        ConnectContextImpl rtCtx = createContext(nextServer.rt, seq + 1);
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
                    producer.send(new ProducerRecord<>(redisTopic, Long.toString(rawMessage.sequenceNumber), rawMessage.content), (event, ex) -> {
                        if (ex != null)
                            ex.printStackTrace();
                        else
                            System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", redisTopic, rawMessage.sequenceNumber, rawMessage.msgType);
                    });
                    producer.send(new ProducerRecord<>(mysqlTopic, Long.toString(rawMessage.sequenceNumber), rawMessage.content), (event, ex) -> {
                        if (ex != null)
                            ex.printStackTrace();
                        else
                            System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", mysqlTopic, rawMessage.sequenceNumber, rawMessage.msgType);
                    });
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        systemMessages.add(new SystemMessage.Restart());
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
            rtCtx.setSequenceNumber(seq + 1);
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