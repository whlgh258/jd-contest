package multithreads;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
public class MultiThreadsController
{
	private static final Logger log = Logger.getLogger( MultiThreadsController.class );
	
	/**
	 * contoller function
	 */
	public void control()
	{
		Connection conn = DBConnection.getConnection();
        Connection[] connections = new Connection[20];

		try
		{
			long CurrentTime = System.currentTimeMillis();

			BlockingQueue<Integer> userQueue = new ArrayBlockingQueue<>( 10 );

            List<Integer> productIds = new ArrayList<>();
            String productSql = "select distinct sku_id from jd.action order by sku_id";
            List<Map<String, Object>> productResult = DBOperation.queryBySql(productSql);
            productIds = new ArrayList<>();
            for(Map<String, Object> productRow : productResult){
                productIds.add((int) productRow.get("sku_id"));
            }

            log.info("product size: " + productIds.size());

            List<Integer> userIds = new ArrayList<>();
            String userSql = "select distinct user_id from jd.action order by user_id";
            List<Map<String, Object>> userResult = DBOperation.queryBySql(conn, userSql);
            log.info("uers size: " + userResult.size());
            for(Map<String, Object> userRow : userResult) {
                int userId = (int) userRow.get("user_id");
                userIds.add(userId);
            }
			
			//start producer
			UserProducer producer = new UserProducer(userQueue, userIds);
			new Thread(producer).start();

            for(int i = 0; i < 20; i++){
                connections[i] = DBConnection.getConnection();
            }
			
			//start consumer
			Thread [] threads = new Thread[20];
			for( int i = 0; i < 20; i++ )
			{
				threads[i] = new Thread(new UserConsumer(userQueue, productIds, connections[i]));
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
	
	public static void main( String[] args )
	{
		MultiThreadsController controller = new MultiThreadsController();
		controller.control();
	}
}


















