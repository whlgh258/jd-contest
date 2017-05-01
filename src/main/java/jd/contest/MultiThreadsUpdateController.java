package jd.contest;

import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


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
public class MultiThreadsUpdateController
{
	private static final Logger log = Logger.getLogger( MultiThreadsUpdateController.class );
	
	/**
	 * contoller function
	 */
	public void control()
	{
        int threadNum = 20;
        int blockSize = 10;
		Connection conn = DBConnection.getConnection();
        Connection[] connections = new Connection[threadNum];

		try
		{
			long CurrentTime = System.currentTimeMillis();

			BlockingQueue<Map<String, Object>> userQueue = new ArrayBlockingQueue<>(blockSize);

            String sql = "select user_id,sku_id,action_date from user_action_1 where click>0";
            List<Map<String, Object>> result = DBOperation.queryBySql(sql);
			//start producer
			UserUpdateProducer producer = new UserUpdateProducer(userQueue, result);
			new Thread(producer).start();

            for(int i = 0; i < connections.length; i++){
                connections[i] = DBConnection.getConnection();
            }
			
			//start consumer
			Thread [] threads = new Thread[threadNum];
			for( int i = 0; i < threads.length; i++ )
			{
				threads[i] = new Thread(new UserUpdateConsumer(userQueue, connections[i]));
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
		MultiThreadsUpdateController controller = new MultiThreadsUpdateController();
		controller.control();
	}
}


















