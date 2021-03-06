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
public class MultiThreadsController
{
	private static final Logger log = Logger.getLogger( MultiThreadsController.class );
	
	/**
	 * contoller function
	 */
	public void control()
	{
		Connection conn = DBConnection.getConnection();
        Connection[] connections = new Connection[1];

		try
		{
			long CurrentTime = System.currentTimeMillis();

			BlockingQueue<Integer> userQueue = new ArrayBlockingQueue<>(1);

            List<Integer> productIds = new ArrayList<>();
            String productSql = "select distinct sku_id from action order by sku_id";
            List<Map<String, Object>> productResult = DBOperation.queryBySql(productSql);
            for(Map<String, Object> productRow : productResult){
                productIds.add((int) productRow.get("sku_id"));
            }

            Map<Integer, Map<String, Object>> userInfo = new HashMap<>();
			Map<Integer, Map<String, Object>> productInfo = new HashMap<>();
            Map<Integer, Map<String, Object>> commentInfo = new HashMap<>();

            String userInfoSql = "select user_id,age,sex,user_level,reg_date from user";
            List<Map<String, Object>> userInfoResult = DBOperation.queryBySql(conn, userInfoSql);
            for(Map<String, Object> userInfoRow : userInfoResult){
                int userId = (int) userInfoRow.get("user_id");
                userInfo.put(userId, userInfoRow);
            }

            String productInfoSql = "select sku_id,attr1,attr2,attr3 from product";
            List<Map<String, Object>> productInfoResult = DBOperation.queryBySql(conn, productInfoSql);
            for(Map<String, Object> productInfoRow : productInfoResult){
                int productId = (int) productInfoRow.get("sku_id");
                productInfo.put(productId, productInfoRow);
            }

            String commentInfoSql = "select sku_id,comment_num,has_bad_comment,bad_comment_rate,last_date from comment order by last_date";
            List<Map<String, Object>> commentInfoResult = DBOperation.queryBySql(conn, commentInfoSql);
            for(Map<String, Object> commentInfoRow : commentInfoResult){
                int productId = (int) commentInfoRow.get("sku_id");
                commentInfo.put(productId, commentInfoRow);
            }

            List<String> dates = new ArrayList<>();
            String dateSql = "select date(min(time)) as min,date(max(time)) as max from action";
            List<Map<String, Object>> dateResult = DBOperation.queryBySql(conn, dateSql);
            if(dateResult.size() > 0){
                Map<String, Object> dateRow = dateResult.get(0);
                Date minDate = (Date) dateRow.get("min");
                Date maxDate = (Date) dateRow.get("max");

                LocalDate min = LocalDate.parse(minDate.toString());
                LocalDate max = LocalDate.parse(maxDate.toString());
                for(LocalDate date = min; date.isBefore(max.plusDays(1)); date = date.plusDays(1)){
                    dates.add(date.toString());
                }
            }

            log.info("user info size: " + userInfo.size());
            log.info("product info size: " + productInfo.size());
            log.info("comment info size: " + commentInfo.size());

            log.info("product size: " + productIds.size());

            List<Integer> userIds = new ArrayList<>();
            String userSql = "select distinct user_id from action order by user_id";
            List<Map<String, Object>> userResult = DBOperation.queryBySql(conn, userSql);
            log.info("uers size: " + userResult.size());
            for(Map<String, Object> userRow : userResult) {
                int userId = (int) userRow.get("user_id");
                userIds.add(userId);
            }
			
			//start producer
			UserProducer producer = new UserProducer(userQueue, userIds);
			new Thread(producer).start();

            for(int i = 0; i < connections.length; i++){
                connections[i] = DBConnection.getConnection();
            }
			
			//start consumer
			Thread [] threads = new Thread[1];
			for( int i = 0; i < threads.length; i++ )
			{
				threads[i] = new Thread(new UserConsumer(userQueue, productIds, userInfo, productInfo, commentInfo, dates, connections[i]));
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
		MultiThreadsController controller = new MultiThreadsController();
		controller.control();
	}
}


















