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
        Connection[] connections = new Connection[20];
        try
        {
            long CurrentTime = System.currentTimeMillis();
            BlockingQueue<Integer> numberQueue = new ArrayBlockingQueue<>(10);

            List<Integer> starts = new ArrayList<>();
            for(int i = 0; i < 3864885; i += 1000){
                starts.add(i);
            }

            //start producer
            NumberProducer producer = new NumberProducer(numberQueue, starts);
            new Thread(producer).start();

            for(int i = 0; i < connections.length; i++){
                connections[i] = DBConnection.getConnection();
            }

            //start consumer
            Thread [] threads = new Thread[20];
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

    private BlockingQueue<Integer> numberQueue;
    private List<Integer> userIds;

    public NumberProducer(BlockingQueue<Integer> numberQueue, List<Integer> userIds)
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
            numberQueue.add(Integer.MAX_VALUE);
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
            if(++i % 1000 == 0){
                log.info("---------------------------------------- " + i + " --------------------------------------------");
            }
            try
            {
                numberQueue.put( userId );
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

    private BlockingQueue<Integer> numberQueue;
    private Connection conn;

    public NumberConsumer(BlockingQueue<Integer> numberQueue, Connection conn) {
        this.numberQueue = numberQueue;
        this.conn = conn;
    }

    @Override
    public void run() {
        boolean doneFlag = false;
        int start = 0;

        while (!doneFlag) {
            try {
                start = numberQueue.take();

                //the last row
                if (start == Integer.MAX_VALUE) {
                    numberQueue.put(Integer.MAX_VALUE);
                    doneFlag = true;
                } else {
                    handle(start);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private void handle(int start) {
        /*String sql = "select user_id,sku_id,click,detail,cart,cart_delete,buy,follow from jd_contest.user_action limit " +  start + ",1000";
        log.info(sql);
        List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
        for(Map<String, Object> row : result){
            int userId = (int) row.get("user_id");
            int productId = (int) row.get("sku_id");
            int click = (int) row.get("click");
            int detail = (int) row.get("detail");
            int cart = (int) row.get("cart");
            int cartDelete = (int) row.get("cart_delete");
            int buy = (int) row.get("buy");
            int follow = (int) row.get("follow");

            *//**
             * buy(4) > cart(2) > cart_delete(3) > follow(5) > click(6) > detail(1)
             *//*
            Date lastActionDate = null;
            String lastSql = "select date(max(time)) as last_date from jd_contest.action where user_id=" + userId + " and sku_id=" + productId + " and type=";
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
            else {
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
        }*/

        String sql = "select user_id,sku_id,click from jd_contest.user_action limit " +  start + ",1000";
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
        }
    }
}