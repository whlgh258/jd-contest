package com.jd.contest;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.log4j.Logger;

/**
 * Created by wanghl on 17-4-7.
 */
public class AddLastDate {
    private static final Logger log = Logger.getLogger(AddLastDate.class);

    public static void main(String[] args) {
        Connection conn = DBConnection.getConnection();
        Connection[] connections = new Connection[50];
        try
        {
            long CurrentTime = System.currentTimeMillis();
            BlockingQueue<String> numberQueue = new ArrayBlockingQueue<>(25);

            /*List<Integer> starts = new ArrayList<>();
            for(int i = 0; i < 3864885; i += 1000){
                starts.add(i);
            }*/

            List<String> list = new ArrayList<>();
            String sql = "select user_id,sku_id from user_action";
            List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
            for(Map<String, Object> row : result){
                int userId = (int) row.get("user_id");
                int productId = (int) row.get("sku_id");
                list.add(userId + "_" + productId);
            }

            //start producer
            NumberProducer producer = new NumberProducer(numberQueue, list);
            new Thread(producer).start();

            for(int i = 0; i < connections.length; i++){
                connections[i] = DBConnection.getConnection();
            }

            //start consumer
            Thread [] threads = new Thread[50];
            for( int i = 0; i < threads.length; i++ )
            {
                threads[i] = new Thread(new NumberConsumer(numberQueue, connections[i]));
                threads[i].start();
            }

            for( int i = 0; i < threads.length; i++ )
            {
                threads[i].join();
            }


            log.info( "The program spent time is: " + ( System.currentTimeMillis() - CurrentTime ) / 60000 );
            log.info( "--------------------------------------run completely---------------------------------------------" );
        }
        catch ( Exception e )
        {
            log.error( e.getMessage() );
        }
        finally
        {
            DBConnection.close( conn );
            for(int i = 0; i < connections.length; i++){
                DBConnection.close(conn);
            }
        }
    }
}

class NumberProducer implements Runnable
{
    private static final Logger log = Logger.getLogger( UserProducer.class );

    private BlockingQueue<String> numberQueue;
    private List<String> userIds;

    public NumberProducer(BlockingQueue<String> numberQueue, List<String> userIds)
    {
        this.numberQueue = numberQueue;
        this.userIds = userIds;
    }

    @Override
    public void run()
    {
        try
        {
            produceUser();
            numberQueue.add("@@@@@");
        }
        catch ( Exception e )
        {
            log.error( e.getMessage() );
        }
    }

    private void produceUser()
    {
        int i = 0;
        for(String key : userIds)
        {
            if(++i % 1000 == 0){
                log.info("---------------------------------------- " + i + " --------------------------------------------");
            }
            try
            {
                numberQueue.put(key);
            }
            catch ( InterruptedException e )
            {
                log.error( e.getMessage(), e );
            }
        }
    }
}

class NumberConsumer implements Runnable {

    private static final Logger log = Logger.getLogger(UserConsumer.class);

    private BlockingQueue<String> numberQueue;
    private Connection conn;

    public NumberConsumer(BlockingQueue<String> numberQueue, Connection conn) {
        this.numberQueue = numberQueue;
        this.conn = conn;
    }

    @Override
    public void run() {
        boolean doneFlag = false;
        String key = "";

        while (!doneFlag) {
            try {
                key = numberQueue.take();

                //the last row
                if (key.equals("@@@@@")) {
                    numberQueue.put("@@@@@");
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
        String sql = "select user_id,sku_id,click,detail,cart,cart_delete,buy,follow from jd_contest.user_action where user_id=" + userId + " and sku_id=" + productId;
        log.info(sql);
        List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
        if(result.size() > 0){
            Map<String, Object> row = result.get(0);
            int click = (int) row.get("click");
            int detail = (int) row.get("detail");
            int cart = (int) row.get("cart");
            int cartDelete = (int) row.get("cart_delete");
            int buy = (int) row.get("buy");
            int follow = (int) row.get("follow");

            /**
             * cart(2) > cart_delete(3) > follow(5) > click(6) > detail(1)
             */
            Date lastActionDate = null;
            String lastSql = "select date(max(time)) as last_date from jd_contest.action where date(time)<='2016-04-10' and user_id=" + userId + " and sku_id=" + productId + " and type=";
            if(buy > 0){
                lastSql += 4;
            }
            else if(cart > 0){
                lastSql += 2;
            }
            else if(cartDelete > 0){
                lastSql += 3;
            }
            else if(follow > 0){
                lastSql += 5;
            }
            else if(click > 0){
                lastSql += 6;
            }
            else if(detail > 0) {
                lastSql += 1;
            }

            List<Map<String, Object>> lastResult = DBOperation.queryBySql(conn, lastSql);
            Date lastDate = null;
            if(lastResult.size() > 0){
                Map<String, Object> lastRow = lastResult.get(0);
                if(lastRow.size() > 0){
                    lastDate = (Date) lastRow.get("last_date");
                    String updateSql = "update jd_contest.user_action set last_action_date='" + lastDate + "' where user_id=" + userId + " and sku_id=" + productId;
                    DBOperation.update(conn, updateSql);
                }
            }
        }

        /*String sql = "select user_id,sku_id,click from jd_contest.user_action limit " +  start + ",1000";
        log.info(sql);
        List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
        for(Map<String, Object> row : result){
            int userId = (int) row.get("user_id");
            int skuId = (int) row.get("sku_id");
            int click = (int) row.get("click");
            if(click > 0){
                String updateSql = "update jd_contest.user_action set has_click=1 where user_id=" + userId + " and sku_id=" + skuId;
                DBOperation.update(conn, updateSql);
            }
        }*/

        /*int userId = Integer.parseInt(key.split("_")[0]);
        int productId = Integer.parseInt(key.split("_")[1]);

        String timeSql = "select date(max(time)) as last_buy_date from action where type=4 and user_id=" + userId + " and sku_id=" + productId;
        List<Map<String, Object>> timeResult = DBOperation.queryBySql(conn, timeSql);
        if(timeResult.size() > 0){
            Map<String, Object> timeRow = timeResult.get(0);
            Date lastBuyDate = null;
            if(timeRow.size() > 0){
                lastBuyDate = (Date) timeRow.get("last_buy_date");
                String updateSql = "update user_action set last_buy_date='" + lastBuyDate + "' where user_id=" + userId + " and sku_id=" + productId;
                DBOperation.update(conn, updateSql);
            }
        }*/
    }
}