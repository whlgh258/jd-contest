package jingdong.contest.test;

import au.com.bytecode.opencsv.CSVWriter;
import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;


/**
 * 
 * Description: Initial work for program running
 * <p/>
 * JDK Version Used: JDK 6.0 
 * <p/>
 * <p/>
 * <p/>
 *
 * @author wanghl 
 * @version 0.1
 * 2012-11-27
 */
public class MultiThreadsControllerTest
{
	private static final Logger log = Logger.getLogger( MultiThreadsControllerTest.class );
	
	/**
	 * contoller function
	 */
	public void control()
	{
        log.info(Runtime.getRuntime().availableProcessors());
		Connection conn = DBConnection.getConnection();
        int queueSize = 10;
        int threadSize = 20;
        Connection[] connections = new Connection[threadSize];

		try
		{
			long CurrentTime = System.currentTimeMillis();

			BlockingQueue<Integer> userQueue = new ArrayBlockingQueue<>(queueSize);

            List<Integer> limits = new ArrayList<>();
            for(int i = 0; i < 1455980; i += 10000){
                limits.add(i);
            }

			//start producer
			UserProducerTest producer = new UserProducerTest(userQueue, limits);
			new Thread(producer).start();

            for(int i = 0; i < connections.length; i++){
                connections[i] = DBConnection.getConnection();
            }

            CSVWriter writer = new CSVWriter( new FileWriter("/home/wanghl/jd_contest/0416/test.csv"), ',', '\0');
            String sql = "select * from user_action_horizon limit 1";
            List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
            List<String> columns = new ArrayList<>();
            if(result.size() > 0){
                Map<String, Object> row = result.get(0);
                row.put("buy", 0);
                for (int j = 1; j <= 5; j++) {
                    row.remove("click_" + j);
                    row.remove("detail_" + j);
                    row.remove("cart_" + j);
                    row.remove("cart_delete_" + j);
                    row.remove("buy_" + j);
                    row.remove("follow_" + j);
                }

                Set<String> keys = row.keySet();
                columns.addAll(keys);
                writer.writeNext(columns.toArray(new String[0]));
            }

			//start consumer
			Thread [] threads = new Thread[threadSize];
			for( int i = 0; i < threads.length; i++ )
			{
				threads[i] = new Thread(new UserConsumerTest(userQueue, connections[i], writer, columns));
				threads[i].start();
			}
			
			for( int i = 0; i < threads.length; i++ )
			{
				threads[i].join();
			}

			log.info( "The program spent time is: " + ( System.currentTimeMillis() - CurrentTime ) /60000 );
			log.info( "--------------------------------------run completely---------------------------------------------" );
		}
		catch ( Exception e )
		{
			log.error( e.getMessage(), e);
		}
		finally
		{
			DBConnection.close( conn );
            for(int i = 0; i < connections.length; i++){
                DBConnection.close(conn);
            }
		}
	}
	
	public static void main( String[] args )
	{
		MultiThreadsControllerTest controller = new MultiThreadsControllerTest();
		controller.control();
	}
}


















