package com.gq;

import java.util.List;

public class Config {
    List<Server> glimpse;
    List<Server> rt;
    Failover failover;
    String kafkaTopic;

    public Config(List<Server> glimpse, List<Server> rt, Failover failover, String kafkaTopic) {
        this.glimpse = glimpse;
        this.rt = rt;
        this.failover = failover;
        this.kafkaTopic = kafkaTopic;
    }

    public Config(){}

    public List<Server> getGlimpse() {
        return glimpse;
    }

    public void setGlimpse(List<Server> glimpse) {
        this.glimpse = glimpse;
    }

    public List<Server> getRt() {
        return rt;
    }

    public void setRt(List<Server> rt) {
        this.rt = rt;
    }

    public Failover getFailover() {
        return failover;
    }

    public void setFailover(Failover failover) {
        this.failover = failover;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public static class Server {
        private String host;
        private int port;
        private String user;
        private String password;

        Server() {}

        public Server(String host, int port, String user, String password) {
            this.host = host;
            this.port = port;
            this.user = user;
            this.password = password;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    public static class Failover {
        private int retry;
        private int waitMs;

        public Failover(){}
        public Failover(int retry, int waitMs) {
            this.retry = retry;
            this.waitMs = waitMs;
        }

        public int getRetry() {
            return retry;
        }

        public int getWaitMs() {
            return waitMs;
        }

        public void setRetry(int retry) {
            this.retry = retry;
        }

        public void setWaitMs(int waitMs) {
            this.waitMs = waitMs;
        }
    }
}

