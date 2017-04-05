package multithreads;

import java.sql.Connection;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

/**
 * 
 * Description: visit alexa and get it's rank
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
public class UserConsumer implements Runnable
{
	private static final Logger log = Logger.getLogger( UserConsumer.class );
	
	private BlockingQueue<Integer> userQueue;
	private List<Integer> productIds;
	private Connection conn;
	
	public UserConsumer(BlockingQueue<Integer> userQueue, List<Integer> productIds, Connection conn )
	{
		this.userQueue = userQueue;
		this.productIds = productIds;
		this.conn = conn;
	}

	@Override
	public void run()
	{
		boolean doneFlag = false;
		int userId = 0;
		
		while( !doneFlag )
		{
			try
			{
				userId = userQueue.take();
				
				//the last row
				if( userId == Integer.MAX_VALUE )
				{
					userQueue.put(Integer.MAX_VALUE);
					doneFlag = true;
				}
				else
				{
					handle( userId );
				}
			}
			catch ( Exception e )
			{
				log.error( e.getMessage(), e );
			}
		}
	}
	
	private void handle(int userId) {
        List<Map<String, Object>> insertList = new ArrayList<>();
        for(int productId : productIds){
            String clickSql = "select count(1) as count from jd.action where type=6 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> clickResult = DBOperation.queryBySql(conn, clickSql);
            if(0 == clickResult.size()){
                continue;
            }

            Map<String, Object> clickRow = clickResult.get(0);
            long click = (long) clickRow.get("count");
            if(0 == click){
                continue;
            }

            String detailSql = "select count(1) as count from jd.action where type=1 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> detailResult = DBOperation.queryBySql(conn, detailSql);

            long detail = 0;
            if(detailResult.size() > 0){
                Map<String, Object> detailRow = detailResult.get(0);
                detail = (long) detailRow.get("count");
            }

            String cartSql = "select count(1) as count from jd.action where type=2 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> cartResult = DBOperation.queryBySql(conn, cartSql);

            long cart = 0;
            if(cartResult.size() > 0){
                Map<String, Object> cartRow = cartResult.get(0);
                cart = (long) cartRow.get("count");
            }

            String cartDeleteSql = "select count(1) as count from jd.action where type=3 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> cartDeleteResult = DBOperation.queryBySql(conn, cartDeleteSql);

            long cartDelete = 0;
            if(cartDeleteResult.size() > 0){
                Map<String, Object> cartDeleteRow = cartDeleteResult.get(0);
                cartDelete = (long) cartDeleteRow.get("count");
            }

            String buySql = "select count(1) as count from jd.action where type=4 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> buyResult = DBOperation.queryBySql(conn, buySql);

            long buy = 0;
            if(buyResult.size() > 0){
                Map<String, Object> buyRow = buyResult.get(0);
                buy = (long) buyRow.get("count");
            }

            String followSql = "select count(1) as count from jd.action where type=5 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> followResult = DBOperation.queryBySql(conn, followSql);

            long follow = 0;
            if(followResult.size() > 0){
                Map<String, Object> followRow = followResult.get(0);
                follow = (long) followRow.get("count");
            }

            Map<String, Object> insertMap = new HashMap<>();
            insertMap.put("user_id", userId);
            insertMap.put("sku_id", productId);
            insertMap.put("click", click);
            insertMap.put("detail", detail);
            insertMap.put("cart", cart);
            insertMap.put("cart_delete", cartDelete);
            insertMap.put("buy", buy);
            insertMap.put("follow", follow);

            insertList.add(insertMap);
        }

        log.info(userId + ": " + insertList.size());
        DBOperation.insert(conn, "jd.user_action", insertList);
	}
}


















