package jd.contest;

import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

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
    private Map<Integer, Map<String, Object>> userInfo;
    private Map<Integer, Map<String, Object>> productInfo;
    private Map<Integer, Map<String, Object>> commentInfo;
    private List<String> dates;
	private Connection conn;
	
	public UserConsumer(BlockingQueue<Integer> userQueue, List<Integer> productIds, Map<Integer, Map<String, Object>> userInfo, Map<Integer, Map<String, Object>> productInfo, Map<Integer, Map<String, Object>> commentInfo, List<String> dates, Connection conn )
	{
		this.userQueue = userQueue;
		this.productIds = productIds;
        this.userInfo = userInfo;
        this.productInfo = productInfo;
        this.commentInfo = commentInfo;
        this.dates = dates;
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
            Map<String, Object> insertMap = new HashMap<>();
            for(String date : dates){
                String sql = "select max(cate) as cate,max(brand) as brand,count(if(type=6,1,null)) as click,count(if(type=1,1,null)) as detail,count(if(type=2,1,null)) as cart,count(if(type=3,1,null)) as cart_delete,count(if(type=4,1,null)) as buy,count(if(type=5,1,null)) as follow from jd_contest.action where user_id=" + userId + " and sku_id=" + productId + " and date(time)='" + date + "'";
                List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
                long click = 0, detail = 0, cart = 0, cartDelete = 0, buy = 0, follow = 0, cate = 0, brand = 0;
                if(result.size() > 0){
                    Map<String, Object> row = result.get(0);
                    if(null != row.get("cate")){
                        cate = (int) row.get("cate");
                    }
                    if(null != row.get("brand")){
                        brand = (int) row.get("brand");
                    }
                    click = (long) row.get("click");
                    detail = (long) row.get("detail");
                    cart = (long) row.get("cart");
                    cartDelete = (long) row.get("cart_delete");
                    buy = (long) row.get("buy");
                    follow = (long) row.get("follow");
                }

                if(0 == click && 0 == detail && 0 == cart && 0 == cartDelete && 0 == buy && 0 == follow){
                    continue;
                }

                log.info(date + ": user_id: " + userId + ", product_id: " + productId + ", click: " + click + ", detail: " + detail + ", cart: " + cart + ", cartDelete: " + cartDelete + ", buy: " + buy + ", follow: " + follow);

                int hasClick = 0;
                if(click >0){
                    hasClick = 1;
                }

                int hasDetail = 0;
                if(detail > 0){
                    hasDetail = 1;
                }

                int hasCart = 0;
                if(cart > 0){
                    hasCart = 1;
                }

                int hasCartDelete = 0;
                if(cartDelete > 0){
                    hasCartDelete = 1;
                }

                int hasBuy = 0;
                if(buy > 0){
                    hasBuy = 1;
                }

                int buyAgain = 0;
                if(buy > 1){
                    buyAgain = 1;
                }

                int hasFollow = 0;
                if(follow > 0){
                    hasFollow = 1;
                }

                Map<String, Object> user = userInfo.get(userId);
                Map<String, Object> product = productInfo.get(productId);
                Map<String, Object> comment = commentInfo.get(productId);

                insertMap.put("user_id", userId);
                insertMap.put("sku_id", productId);
                insertMap.put("click", click);
                insertMap.put("has_click", hasClick);
//            insertMap.put("model_id", modelIds);
                insertMap.put("detail", detail);
                insertMap.put("has_detail", hasDetail);
                insertMap.put("cart", cart);
                insertMap.put("has_cart", hasCart);
                insertMap.put("cart_delete", cartDelete);
                insertMap.put("has_cart_delete", hasCartDelete);
                insertMap.put("buy", buy);
                insertMap.put("has_buy", hasBuy);
                insertMap.put("buy_again", buyAgain);
                insertMap.put("follow", follow);
                insertMap.put("has_follow", hasFollow);
                insertMap.put("cate", cate);
                insertMap.put("brand", brand);

                if(null != user){
                    insertMap.put("age", user.get("age"));
                    insertMap.put("sex", user.get("sex"));
                    insertMap.put("user_level", user.get("user_level"));
                    insertMap.put("reg_date", user.get("reg_date"));
                }
                else {
                    insertMap.put("age", 0);
                    insertMap.put("sex", 2);
                    insertMap.put("user_level", 0);
                    insertMap.put("reg_date", null);
                }

                if(null != product){
                    insertMap.put("attr1", product.get("attr1"));
                    insertMap.put("attr2", product.get("attr2"));
                    insertMap.put("attr3", product.get("attr3"));
                }
                else {
                    insertMap.put("attr1", 0);
                    insertMap.put("attr2", 0);
                    insertMap.put("attr3", 0);
                }

                if(null != comment){
                    insertMap.put("comment_num", comment.get("comment_num"));
                    insertMap.put("has_bad_comment", comment.get("has_bad_comment"));
                    insertMap.put("bad_comment_rate", comment.get("bad_comment_rate"));
                    insertMap.put("last_comment_date", comment.get("last_date"));
                }
                else {
                    insertMap.put("comment_num", 0);
                    insertMap.put("has_bad_comment", 0);
                    insertMap.put("bad_comment_rate", 0.0);
                    insertMap.put("last_comment_date", null);
                }

                insertMap.put("action_date", date);

                insertList.add(insertMap);
            }
        }


        log.info(userId + ": " + insertList.size());
        DBOperation.insert(conn, "user_action_1", insertList);
	}
}


















