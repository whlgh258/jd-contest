package com.jd.contest;

import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by wanghl on 17-4-9.
 */
public class UpdateCateAndBrand {
    private static final Logger log = Logger.getLogger(UpdateCateAndBrand.class);

    public static void main(String[] args)  throws Exception{
        Connection conn = DBConnection.getConnection();
        Connection[] connections = new Connection[20];
        long CurrentTime = System.currentTimeMillis();

        BlockingQueue<String> userQueue = new ArrayBlockingQueue<>( 10 );

        String sql = "select user_id,sku_id from user_action;";
        List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
        List<String> list = new ArrayList<>();
        for(Map<String, Object> row : result){
            int userId = (int) row.get("user_id");
            int productId = (int) row.get("sku_id");
            list.add(userId + "_" + productId);
        }

        log.info("has action user and product: " + list.size());

        //start producer
        UpdateProducer producer = new UpdateProducer(userQueue, list);
        new Thread(producer).start();

        for(int i = 0; i < 20; i++){
            connections[i] = DBConnection.getConnection();
        }

        Thread [] threads = new Thread[20];
        for( int i = 0; i < 20; i++ )
        {
            threads[i] = new Thread(new UpdateConsumer(userQueue, connections[i]));
            threads[i].start();
        }

        for( int i = 0; i < threads.length; i++ )
        {
            threads[i].join();
        }

        DBConnection.close(conn);

        for(int i = 0; i < connections.length; i++){
            DBConnection.close(conn);
        }
    }
}

class UpdateProducer implements Runnable{
    private static final Logger log = Logger.getLogger(UserBuyProducer.class);

    private BlockingQueue<String> userQueue;
    private List<String> keys;

    public UpdateProducer(BlockingQueue<String> userQueue, List<String> keys)
    {
        this.userQueue = userQueue;
        this.keys = keys;
    }

    @Override
    public void run()
    {
        try
        {
            produceUser();
            userQueue.add("@@@@@");
        }
        catch ( Exception e )
        {
            log.error( e.getMessage() );
        }
    }

    private void produceUser()
    {
        int i = 0;
        for( String key : keys )
        {
            if(++i % 100 == 0){
                log.info("---------------------------------------- " + i + " --------------------------------------------");
            }
            try
            {
                userQueue.put( key );
            }
            catch ( InterruptedException e )
            {
                log.error( e.getMessage(), e );
            }
        }
    }
}

class UpdateConsumer implements Runnable{
    private static final Logger log = Logger.getLogger(UserBuyConsumer.class);

    private BlockingQueue<String> userQueue;
    private Connection conn;

    public UpdateConsumer(BlockingQueue<String> userQueue, Connection conn){
        this.userQueue = userQueue;
        this.conn = conn;
    }

    @Override
    public void run() {
        boolean doneFlag = false;
        String key = "";

        while (!doneFlag) {
            try {
                key = userQueue.take();

                //the last row
                if (key.equals("@@@@@")) {
                    userQueue.put("@@@@@");
                    doneFlag = true;
                } else {
                    handle(key);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private void handle(String key) {
        int userId = Integer.parseInt(key.split("_")[0]);
        int productId = Integer.parseInt(key.split("_")[1]);
        log.info(userId + ": " + productId);
        String querySql = "select cate,brand from action where user_id=" + userId + " and sku_id=" + productId;
        List<Map<String, Object>> queryResult = DBOperation.queryBySql(conn, querySql);
        if(queryResult.size() > 0){
            Map<String, Object> queryRow = queryResult.get(0);
            int cate = (int) queryRow.get("cate");
            int brand = (int) queryRow.get("brand");

            String updateSql = "update user_action set cate=" + cate + ",brand=" + brand + " where user_id=" + userId + " and sku_id=" + productId;
            DBOperation.update(conn, updateSql);
        }
    }
}