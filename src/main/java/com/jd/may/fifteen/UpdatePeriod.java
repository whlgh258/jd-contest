package com.jd.may.fifteen;

import java.sql.Connection;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.log4j.Logger;

/**
 * Created by wanghl on 17-5-15.
 */
public class UpdatePeriod {
    private static final Logger log = Logger.getLogger(UpdatePeriod.class);

    public static void main(String[] args) throws InterruptedException {
        int threadNum = 60;
        int blockSize = 30;
        Connection conn = DBConnection.getConnection();
        Connection[] connections = new Connection[threadNum];
        String tablename = "user_action_2";

        BlockingQueue<Map<String, Object>> userQueue = new ArrayBlockingQueue<>(blockSize);


        String sql = "select id,action_date from " + tablename;
        List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);

        //start producer
        UpdatePeriodProducer producer = new UpdatePeriodProducer(userQueue, result);
        new Thread(producer).start();

        for(int i = 0; i < connections.length; i++){
            connections[i] = DBConnection.getConnection();
        }

        //start consumer
        Thread [] threads = new Thread[threadNum];
        for( int i = 0; i < threads.length; i++ )
        {
            threads[i] = new Thread(new UpdatePeriodConsumer(userQueue, connections[i], tablename));
            threads[i].start();
        }

        for( int i = 0; i < threads.length; i++ )
        {
            threads[i].join();
        }

        DBConnection.close( conn );
        for(int i = 0; i < connections.length; i++){
            DBConnection.close(conn);
        }
    }
}

class UpdatePeriodProducer implements Runnable {

    private static final Logger log = Logger.getLogger(UpdatePeriodProducer.class);

    private BlockingQueue<Map<String, Object>> userQueue;

    private List<Map<String, Object>> result;

    public UpdatePeriodProducer(BlockingQueue<Map<String, Object>> userQueue, List<Map<String, Object>> result) {
        this.userQueue = userQueue;
        this.result = result;
    }

    @Override
    public void run() {
        try {
            produceUser();
            userQueue.add(null);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void produceUser() {
        int i = 0;
        for (Map<String, Object> row : result) {
            log.info(++i);
            try {
                userQueue.put(row);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}

class UpdatePeriodConsumer implements Runnable
{
    private static final Logger log = Logger.getLogger(UpdatePeriodConsumer.class);

    private BlockingQueue<Map<String, Object>> userQueue;
    private Connection conn;
    private String tablename;

    public UpdatePeriodConsumer(BlockingQueue<Map<String, Object>> userQueue, Connection conn, String tablename )
    {
        this.userQueue = userQueue;
        this.conn = conn;
        this.tablename = tablename;
    }

    @Override
    public void run()
    {
        boolean doneFlag = false;
        Map<String, Object> row = null;

        while( !doneFlag )
        {
            try
            {
                row = userQueue.take();

                //the last row
                if( row == null )
                {
                    userQueue.put(null);
                    doneFlag = true;
                }
                else
                {
                    handle( row );
                }
            }
            catch ( Exception e )
            {
                log.error( e.getMessage(), e );
            }
        }
    }

    private void handle(Map<String, Object> row) {
        int period = 0;
        int id = (int) row.get("id");
        Date actionDate = (Date) row.get("action_date");
        LocalDate date = actionDate.toLocalDate();
        LocalDate start = LocalDate.parse("2016-02-01");
        LocalDate end = LocalDate.parse("2016-04-16");
        for(LocalDate begin = start; begin.isBefore(end); begin = begin.plusDays(5L)){
            if(date.isAfter(begin.minusDays(1L)) && date.isBefore(begin.plusDays(5L))){
                period = (end.getDayOfYear() - begin.getDayOfYear()) / 5;
                String updateSql = "update " + tablename + " set action_period=" + period + " where id=" + id;
                DBOperation.update(conn, updateSql);
                break;
            }
        }
    }
}

















