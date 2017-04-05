package com.jd.contest;

import multithreads.DBOperation;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import scala.Int;

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
	private Connection conn;
	
	public UserConsumer(BlockingQueue<Integer> userQueue, List<Integer> productIds, Map<Integer, Map<String, Object>> userInfo, Map<Integer, Map<String, Object>> productInfo, Map<Integer, Map<String, Object>> commentInfo, Connection conn )
	{
		this.userQueue = userQueue;
		this.productIds = productIds;
        this.userInfo = userInfo;
        this.productInfo = productInfo;
        this.commentInfo = commentInfo;
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
            String clickSql = "select count(1) as count from jd_contest.action where type=6 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> clickResult = DBOperation.queryBySql(conn, clickSql);
//            if(0 == clickResult.size()){
//                continue;
//            }

            long click = 0;
            if(clickResult.size() > 0){
                Map<String, Object> clickRow = clickResult.get(0);
                click = (long) clickRow.get("count");
            }
//            if(0 == click){
//                continue;
//            }

            int hasClick = 1;

            String clickModelIds = "";
            String clickModelSql = "select model_id from jd_contest.action where type=6 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> clickModelResult = DBOperation.queryBySql(conn, clickModelSql);
            for(Map<String, Object> clickModelRow : clickModelResult){
                int modelId = (int) clickModelRow.get("model_id");
                if(modelId > 0){
                    clickModelIds += modelId + ",";
                }
            }

            StringUtils.removeEnd(clickModelIds, ",");

            String detailSql = "select count(1) as count from jd_contest.action where type=1 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> detailResult = DBOperation.queryBySql(conn, detailSql);

            long detail = 0;
            if(detailResult.size() > 0){
                Map<String, Object> detailRow = detailResult.get(0);
                detail = (long) detailRow.get("count");
            }

            int hasDetail = 0;
            if(detail > 0){
                hasDetail = 1;
            }

            String detailModelIds = "";
            String detailModelSql = "select model_id from jd_contest.action where type=1 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> detailModelResult = DBOperation.queryBySql(conn, detailModelSql);
            for(Map<String, Object> detailModelRow : detailModelResult){
                int modelId = (int) detailModelRow.get("model_id");
                if(modelId > 0){
                    detailModelIds += modelId + ",";
                }
            }

            StringUtils.removeEnd(detailModelIds, ",");

            String cartSql = "select count(1) as count from jd_contest.action where type=2 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> cartResult = DBOperation.queryBySql(conn, cartSql);

            long cart = 0;
            if(cartResult.size() > 0){
                Map<String, Object> cartRow = cartResult.get(0);
                cart = (long) cartRow.get("count");
            }

            int hasCart = 0;
            if(cart > 0){
                hasCart = 1;
            }

            String cartModelIds = "";
            String cartModelSql = "select model_id from jd_contest.action where type=2 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> cartModelResult = DBOperation.queryBySql(conn, cartModelSql);
            for(Map<String, Object> cartModelRow : cartModelResult){
                int modelId = (int) cartModelRow.get("model_id");
                if(modelId > 0){
                    cartModelIds += modelId + ",";
                }
            }

            StringUtils.removeEnd(cartModelIds, ",");

            String cartDeleteSql = "select count(1) as count from jd_contest.action where type=3 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> cartDeleteResult = DBOperation.queryBySql(conn, cartDeleteSql);

            long cartDelete = 0;
            if(cartDeleteResult.size() > 0){
                Map<String, Object> cartDeleteRow = cartDeleteResult.get(0);
                cartDelete = (long) cartDeleteRow.get("count");
            }

            int hasCartDelete = 0;
            if(cartDelete > 0){
                hasCartDelete = 1;
            }

            String cartDeleteModelIds = "";
            String cartDeleteModelSql = "select model_id from jd_contest.action where type=3 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> cartDeleteModelResult = DBOperation.queryBySql(conn, cartDeleteModelSql);
            for(Map<String, Object> cartDeleteModelRow : cartDeleteModelResult){
                int modelId = (int) cartDeleteModelRow.get("model_id");
                if(modelId > 0){
                    cartDeleteModelIds += modelId + ",";
                }
            }

            StringUtils.removeEnd(cartDeleteModelIds, ",");

            String buySql = "select count(1) as count from jd_contest.action where type=4 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> buyResult = DBOperation.queryBySql(conn, buySql);

            long buy = 0;
            if(buyResult.size() > 0){
                Map<String, Object> buyRow = buyResult.get(0);
                buy = (long) buyRow.get("count");
            }

            int hasBuy = 0;
            if(buy > 0){
                hasBuy = 1;
            }

            int buyAgain = 0;
            if(buy > 1){
                buyAgain = 1;
            }

            String buyModelIds = "";
            String buyModelSql = "select model_id from jd_contest.action where type=4 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> buyModelResult = DBOperation.queryBySql(conn, buyModelSql);
            for(Map<String, Object> buyModelRow : buyModelResult){
                int modelId = (int) buyModelRow.get("model_id");
                if(modelId > 0){
                    buyModelIds += modelId + ",";
                }
            }

            StringUtils.removeEnd(buyModelIds, ",");

            String followSql = "select count(1) as count from jd_contest.action where type=5 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> followResult = DBOperation.queryBySql(conn, followSql);

            long follow = 0;
            if(followResult.size() > 0){
                Map<String, Object> followRow = followResult.get(0);
                follow = (long) followRow.get("count");
            }

            int hasFollow = 0;
            if(follow > 0){
                hasFollow = 1;
            }

            String followModelIds = "";
            String followModelSql = "select model_id from jd_contest.action where type=5 and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> followModelResult = DBOperation.queryBySql(conn, followModelSql);
            for(Map<String, Object> followModelRow : followModelResult){
                int modelId = (int) followModelRow.get("model_id");
                if(modelId > 0){
                    followModelIds += modelId + ",";
                }
            }

            StringUtils.removeEnd(followModelIds, ",");

            if(0 == click && 0 == detail && 0 == cart && 0 == cartDelete && 0 == buy && 0 == follow){
                continue;
            }

            log.info("user_id: " + userId + ", product_id: " + productId + ", click: " + click + ", detail: " + detail + ", cart: " + cart + ", cartDelete: " + cartDelete + ", buy: " + buy + ", follow: " + follow);
            Map<String, Object> user = userInfo.get(userId);
            Map<String, Object> product = productInfo.get(productId);
            Map<String, Object> comment = commentInfo.get(productId);

            Map<String, Object> insertMap = new HashMap<>();
            insertMap.put("user_id", userId);
            insertMap.put("sku_id", productId);
            insertMap.put("click", click);
            insertMap.put("has_click", hasClick);
            insertMap.put("click_model_id", clickModelIds);
            insertMap.put("detail", detail);
            insertMap.put("has_detail", hasDetail);
            insertMap.put("detail_model_id", detailModelIds);
            insertMap.put("cart", cart);
            insertMap.put("has_cart", hasCart);
            insertMap.put("cart_model_id", cartModelIds);
            insertMap.put("cart_delete", cartDelete);
            insertMap.put("has_cart_delete", hasCartDelete);
            insertMap.put("cart_delete_model_id", cartDeleteModelIds);
            insertMap.put("buy", buy);
            insertMap.put("has_buy", hasBuy);
            insertMap.put("buy_again", buyAgain);
            insertMap.put("buy_model_id", buyModelIds);
            insertMap.put("follow", follow);
            insertMap.put("has_follow", hasFollow);
            insertMap.put("follow_model_id", followModelIds);
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
                insertMap.put("cate", product.get("cate"));
                insertMap.put("brand", product.get("brand"));
            }
            else {
                insertMap.put("attr1", 0);
                insertMap.put("attr2", 0);
                insertMap.put("attr3", 0);
                insertMap.put("cate", 0);
                insertMap.put("brand", 0);
            }
            if(null != comment){
                insertMap.put("comment_num", comment.get("comment_num"));
                insertMap.put("has_bad_comment", comment.get("has_bad_comment"));
                insertMap.put("bad_comment_rate", comment.get("bad_comment_rate"));
                insertMap.put("last_date", comment.get("last_date"));
            }
            else {
                insertMap.put("comment_num", 0);
                insertMap.put("has_bad_comment", 0);
                insertMap.put("bad_comment_rate", 0.0);
                insertMap.put("last_date", null);
            }

            insertList.add(insertMap);
        }

        log.info(userId + ": " + insertList.size());
        DBOperation.insert(conn, "jd_contest.user_action", insertList);
	}
}


















