package com.hazelcast.jet;

import com.hazelcast.jet.dto.Order;
import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import redis.clients.jedis.*;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

public class JedisHelper {

    private Jedis jedis;
    private Pipeline pipelined;
    private JedisPool jedisPool;
    private String hashScript;

    String redisHost = "redis://localhost:6379";

    private static final int NUM_OF_CMDS = 1000;


    private Queue<Order> subtractRequestQueue = new ConcurrentLinkedQueue<>();

    private static final String SUBTRACT_BALANCE_LUA_SCRIPT = "local balance=redis.call('get', KEYS[1]) " +
            "if balance and (balance - ARGV[1]) >= 0 " +
            "then redis.call('incrby', KEYS[1], - ARGV[1]) return true " +
            "else return false end";
    private static final String BALANCE_KEY_FORMAT = "balance#$uid";


    private static class UdsJedisSocketFactory implements JedisSocketFactory {

        private static final File UDS_SOCKET = new File("/var/run/redis/redis.sock");

        @Override
        public Socket createSocket() throws IOException {
            Socket socket = AFUNIXSocket.newStrictInstance();
            socket.connect(new AFUNIXSocketAddress(UDS_SOCKET), Protocol.DEFAULT_TIMEOUT);
            return socket;
        }

        @Override
        public String getDescription() {
            return UDS_SOCKET.toString();
        }

        @Override
        public String getHost() {
            return UDS_SOCKET.toString();
        }

        @Override
        public void setHost(String host) {
        }

        @Override
        public int getPort() {
            return 0;
        }

        @Override
        public void setPort(int port) {
        }

        @Override
        public int getConnectionTimeout() {
            return Protocol.DEFAULT_TIMEOUT;
        }

        @Override
        public void setConnectionTimeout(int connectionTimeout) {
        }

        @Override
        public int getSoTimeout() {
            return Protocol.DEFAULT_TIMEOUT;
        }

        @Override
        public void setSoTimeout(int soTimeout) {
        }
    }


    @PostConstruct
    public void initialize() {
        jedisPool = new JedisPool(redisHost);

        jedis = jedisPool.getResource();
       // jedis = new Jedis(new UdsJedisSocketFactory());
        pipelined = jedis.pipelined();
        hashScript = jedis.scriptLoad(SUBTRACT_BALANCE_LUA_SCRIPT);
        new Thread(() -> {
            System.out.println("Start sync pipeline job");
            while(true) {
                doSyncPipeline();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        //LOG.info("test");

    }

    public void subtractBalance(Order order) {
        synchronized (pipelined) {
            subtractRequestQueue.add(order);
            pipelined.evalsha(hashScript, Collections.singletonList(getBalanceKey(order.getUid())),
                    Collections.singletonList(String.valueOf(order.getAmount())));
        }
    }

    public void doSyncPipeline() {
        System.out.println(subtractRequestQueue.size());
        if (subtractRequestQueue.size() > 0) {
            int numOfReqs = subtractRequestQueue.size();
            String[] orderIds = new String[numOfReqs];
            for (int index = 0; index < numOfReqs; index ++) {
                Order order = subtractRequestQueue.poll();
                orderIds[index] = order.getId();
            }
            synchronized (pipelined) {
                long start = System.currentTimeMillis();
                List<Object> responseList = pipelined.syncAndReturnAll();
                long time = System.currentTimeMillis() - start;
                System.out.println("Took: " + time + " size " + responseList.size());
            }



           // for (int i = 0; i < responseList.size(); i++) {
          //      //LOG.info("order id: " + orderIds[i] + " ,result: " + responseList.get(i));
         //   }
        }
    }

    private String getBalanceKey(String uid) {
        return BALANCE_KEY_FORMAT.replace("$uid", uid);
    }
}
