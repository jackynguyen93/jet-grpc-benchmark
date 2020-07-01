package com.hazelcast.jet;

import com.hazelcast.jet.dto.Order;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

public class JedisHelper {

    private Jedis jedis;
    private Pipeline pipelined;
    private JedisPool jedisPool;

    String redisHost = "redis://localhost:6379";

    private static final int NUM_OF_CMDS = 1000;


    private Queue<Order> subtractRequestQueue = new ConcurrentLinkedQueue<>();

    private static final String SUBTRACT_BALANCE_LUA_SCRIPT = "local balance=redis.call('get', KEYS[1]) " +
            "if balance and (balance - ARGV[1]) >= 0 " +
            "then redis.call('set', KEYS[1], balance - ARGV[1]) return true " +
            "else return false end";
    private static final String BALANCE_KEY_FORMAT = "balance#$uid";

    @PostConstruct
    public void initialize() {
        jedisPool = new JedisPool(redisHost);
        jedis = jedisPool.getResource();
        pipelined = jedis.pipelined();
        new Thread(() -> {
            System.out.println("Start sync pipeline job");
            while(true) {
                doSyncPipeline();
              /*  try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/
            }
        }).start();
        //LOG.info("test");

    }

    public void subtractBalance(Order order) {
        subtractRequestQueue.add(order);
    }

    public void doSyncPipeline() {
        System.out.println(subtractRequestQueue.size());
        if (subtractRequestQueue.size() > 0) {
            int numOfReqs = subtractRequestQueue.size();
            String[] orderIds = new String[numOfReqs];
            for (int index = 0; index < numOfReqs; index ++) {
                long time = System.currentTimeMillis();
            //    Order order = subtractRequestQueue.poll();
               // Order order = new Order()
              //  orderIds[index] = order.getId();
                pipelined.eval(SUBTRACT_BALANCE_LUA_SCRIPT, Collections.singletonList(getBalanceKey("u1")),
                        Collections.singletonList(String.valueOf("1")));
            }
           // List<Object> responseList = pipelined.syncAndReturnAll();

           // for (int i = 0; i < responseList.size(); i++) {
          //      //LOG.info("order id: " + orderIds[i] + " ,result: " + responseList.get(i));
         //   }
        }
    }

    private String getBalanceKey(String uid) {
        return BALANCE_KEY_FORMAT.replace("$uid", uid);
    }
}
