package contest.jd;

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
public class MultiThreadsControllerHorizon
{
	private static final Logger log = Logger.getLogger( MultiThreadsControllerHorizon.class );
	
	/**
	 * contoller function
	 */
	public void control()
	{
		Connection conn = DBConnection.getConnection();
        int threadNum = 16;
        int queueSize = 8;
        Connection[] connections = new Connection[threadNum];

		try
		{
			long CurrentTime = System.currentTimeMillis();

			BlockingQueue<Integer> userQueue = new ArrayBlockingQueue<>(queueSize);

            List<Integer> productIds = new ArrayList<>();
            String productSql = "select distinct sku_id from action_1 order by sku_id";
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
            Map<String, Integer> dateSegment = new LinkedHashMap<>();
            String dateSql = "select date(min(time)) as min,date(max(time)) as max from action_1 where date>='2016-02-01'";
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

                int i = 0;
                for(LocalDate date = max.plusDays(5); date.isAfter(min); date = date.minusDays(5)){
                    ++i;
                    dateSegment.put(date.toString(), i);
                    dateSegment.put(date.minusDays(1).toString(), i);
                    dateSegment.put(date.minusDays(2).toString(), i);
                    dateSegment.put(date.minusDays(3).toString(), i);
                    dateSegment.put(date.minusDays(4).toString(), i);
                }
            }

            Map<String, String> detailMap = new HashMap<>();
            String detailSql = "select user_id,sku_id,date(time) as date,type,count(1) as count,max(cate) as cate,max(brand) as brand,group_concat(model_id) as model_id from action_1 group by user_id,sku_id,date(time),type having count>0";
            List<Map<String, Object>> detailResult = DBOperation.queryBySql(conn, detailSql);
            for(Map<String, Object> detailRow : detailResult){
                int userId = (int) detailRow.get("user_id");
                int productId = (int) detailRow.get("sku_id");
                String date = ((Date)detailRow.get("date")).toString();
                int type = (int) detailRow.get("type");
                long count = (long) detailRow.get("count");
                int cate = (int) detailRow.get("cate");
                int brand = (int) detailRow.get("brand");
                String modelId = (String) detailRow.get("model_id");

                String key = userId + "_" + productId + "_" + date + "_" + type;
                String value = count + "_" + cate + "_" + brand + "_" + modelId;
                detailMap.put(key, value);
            }

            log.info("user info size: " + userInfo.size());
            log.info("product info size: " + productInfo.size());
            log.info("comment info size: " + commentInfo.size());

            log.info("product size: " + productIds.size());
            log.info("detail size: " + detailMap.size());

            List<Integer> userIds = new ArrayList<>();
            String userSql = "select distinct user_id from action_1 order by user_id";
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
			Thread [] threads = new Thread[threadNum];
			for( int i = 0; i < threads.length; i++ )
			{
				threads[i] = new Thread(new UserConsumer(userQueue, productIds, userInfo, productInfo, commentInfo, dates, dateSegment, detailMap, connections[i]));
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
		MultiThreadsControllerHorizon controller = new MultiThreadsControllerHorizon();
		controller.control();
	}
}


















