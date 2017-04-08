package com.jd.contest;

import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.log4j.Logger;
import scala.Int;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by wanghl on 17-4-8.
 */
public class UserBuySimilarity {
    private static final Logger log = Logger.getLogger(UserBuySimilarity.class);

    public static void main(String[] args) {
        Connection conn = DBConnection.getConnection();
        Connection[] connections = new Connection[20];
        long CurrentTime = System.currentTimeMillis();

        try {
            BlockingQueue<Integer> userQueue = new ArrayBlockingQueue<>( 10 );

            List<Integer> userIds = new ArrayList<>();
            String userInfoSql = "select distinct user_id from jd_contest.user_action where buy>0 order by user_id";
            List<Map<String, Object>> userInfoResult = DBOperation.queryBySql(conn, userInfoSql);
            for (Map<String, Object> userInfoRow : userInfoResult) {
                int userId = (int) userInfoRow.get("user_id");
                userIds.add(userId);
            }

            List<Integer> productIds = new ArrayList<>();
            String productSql = "select distinct sku_id from jd_contest.user_action where buy>0 order by sku_id";
            List<Map<String, Object>> productResult = DBOperation.queryBySql(productSql);
            for (Map<String, Object> productRow : productResult) {
                productIds.add((int) productRow.get("sku_id"));
            }

            Map<String, Integer> buyMap = new HashMap<>();
            String buySql = "select user_id,sku_id,buy from jd_contest.user_action where buy>0";
            List<Map<String, Object>> buyResult = DBOperation.queryBySql(conn, buySql);
            for(Map<String, Object> buyRow : buyResult){
                int userId = (int) buyRow.get("user_id");
                int productId = (int) buyRow.get("sku_id");
                int buy = (int) buyRow.get("buy");
                String key = userId + "_" + productId;
                buyMap.put(key, buy);
            }

            log.info("user ids: " + userIds.size());
            log.info("product ids: " + productIds.size());
            log.info("has buy user and product: " + buyMap.size());

            //start producer
            UserBuyProducer producer = new UserBuyProducer(userQueue, userIds);
            new Thread(producer).start();

            for(int i = 0; i < 20; i++){
                connections[i] = DBConnection.getConnection();
            }

            Thread [] threads = new Thread[20];
            for( int i = 0; i < 20; i++ )
            {
                threads[i] = new Thread(new UserBuyConsumer(userQueue, userIds, productIds, buyMap, connections[i]));
                threads[i].start();
            }

            for( int i = 0; i < threads.length; i++ )
            {
                threads[i].join();
            }
        }
        catch (Exception e){
            log.error(e.getMessage(),e );
        }
        finally {
            DBConnection.close( conn );
            for(int i = 0; i < connections.length; i++){
                DBConnection.close(conn);
            }
        }


        log.info( "The program spent time is: " + ( System.currentTimeMillis() - CurrentTime ) /60000 );
        log.info( "--------------------------------------run completely---------------------------------------------" );
    }
}

class UserBuyProducer implements Runnable{
    private static final Logger log = Logger.getLogger(UserBuyProducer.class);

    private BlockingQueue<Integer> userQueue;
    private List<Integer> userIds;

    public UserBuyProducer(BlockingQueue<Integer> userQueue, List<Integer> userIds)
    {
        this.userQueue = userQueue;
        this.userIds = userIds;
    }

    @Override
    public void run()
    {
        try
        {
            produceUser();
            userQueue.add(Integer.MAX_VALUE);
        }
        catch ( Exception e )
        {
            log.error( e.getMessage() );
        }
    }

    private void produceUser()
    {
        int i = 0;
        for( int userId : userIds )
        {
            if(++i % 100 == 0){
                log.info("---------------------------------------- " + i + " --------------------------------------------");
            }
            try
            {
                userQueue.put( userId );
            }
            catch ( InterruptedException e )
            {
                log.error( e.getMessage(), e );
            }
        }
    }
}

class UserBuyConsumer implements Runnable{
    private static final Logger log = Logger.getLogger(UserBuyConsumer.class);

    private BlockingQueue<Integer> userQueue;
    private List<Integer> userIds;
    private List<Integer> productIds;
    private Map<String, Integer> buyMap;
    private Connection conn;

    public UserBuyConsumer(BlockingQueue userQueue, List<Integer> userIds, List<Integer> productIds, Map<String, Integer> buyMap, Connection conn){
        this.userQueue = userQueue;
        this.userIds = userIds;
        this.productIds = productIds;
        this.buyMap = buyMap;
        this.conn = conn;
    }

    @Override
    public void run() {
        boolean doneFlag = false;
        int userId = 0;

        while (!doneFlag) {
            try {
                userId = userQueue.take();

                //the last row
                if (userId == Integer.MAX_VALUE) {
                    userQueue.put(Integer.MAX_VALUE);
                    doneFlag = true;
                } else {
                    handle(userId);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private void handle(int userIdA) {
        RealVector vectorA = null;
        double[] arrayA = new double[productIds.size()];
        for (int i = 0; i < productIds.size(); i++) {
            int productId = productIds.get(i);
            String key = userIdA + "_" + productId;
            Integer buy = buyMap.get(key);
            if(null != buy){
                arrayA[i] = buy;
            }
        }

        vectorA = new ArrayRealVector(arrayA);
        if(0 == StatUtils.sum(vectorA.toArray())){
            return;
        }

        List<Map<String, Object>> insertList = new ArrayList<>();
        for (int userIdB : userIds) {
            RealVector vectorB = null;
            double[] arrayB = new double[productIds.size()];
            for (int i = 0; i < productIds.size(); i++) {
                int productId = productIds.get(i);
                String key = userIdB + "_" + productId;
                Integer buy = buyMap.get(key);
                if(null != buy){
                    arrayB[i] = buy;
                }
            }

            vectorB = new ArrayRealVector(arrayB);
            if(0 == StatUtils.sum(vectorB.toArray())){
                return;
            }

            double similarity = vectorA.cosine(vectorB);
            if(similarity > 0){
                Map<String, Object> insertMap = new HashMap<>();
                insertMap.put("user_idA", userIdA);
                insertMap.put("user_idB", userIdB);
                insertMap.put("similarity", similarity);

                insertList.add(insertMap);
            }
        }

        DBOperation.insert(conn, "jd_contest.user_buy_similarity", insertList);
    }
}
