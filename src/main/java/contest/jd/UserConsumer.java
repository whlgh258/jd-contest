package contest.jd;

import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
    private Map<String, Integer> dateSegment;
    private Map<String, String> detailMap;
	private Connection conn;
	
	public UserConsumer(BlockingQueue<Integer> userQueue, List<Integer> productIds, Map<Integer, Map<String, Object>> userInfo, Map<Integer, Map<String, Object>> productInfo, Map<Integer, Map<String, Object>> commentInfo, List<String> dates, Map<String, Integer> dateSegment, Map<String, String> detailMap, Connection conn )
	{
		this.userQueue = userQueue;
		this.productIds = productIds;
        this.userInfo = userInfo;
        this.productInfo = productInfo;
        this.commentInfo = commentInfo;
        this.dates = dates;
        this.dateSegment = dateSegment;
        this.detailMap = detailMap;
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
        for(int productId : productIds){
            Map<String, Object> insertMap = new HashMap<>();
            LinkedHashMap<String, Integer> map = new LinkedHashMap();
            long click = 0, detail = 0, cart = 0, cartDelete = 0, buy = 0, follow = 0, cate = 0, brand = 0;
            boolean hasAction = false;
            Map<String, Long> actionAccu = new HashMap<>();
            for(String date : dates){
                click = detail = cart = cartDelete = buy = follow = cate = brand = 0;
                String modelId = "";
                for(int type = 1; type <= 6; type++) {
                    String key = userId + "_" + productId + "_" + date + "_" + type;
                    String value = detailMap.get(key);

                    if(null != value){
                        String[] parts = value.split("_");
                        int count = Integer.parseInt(parts[0]);
                        cate = Integer.parseInt(parts[1]);
                        brand = Integer.parseInt(parts[2]);
                        modelId = parts[3];

                        if(1 == type) {
                            detail = count;
                        }
                        else if(2 ==type) {
                            cart = count;
                        }
                        else if(3 == type) {
                            cartDelete = count;
                        }
                        else if(4 == type) {
                            buy = count;
                        }
                        else if(5 == type) {
                            follow = count;
                        }
                        else if(6 == type) {
                            click = count;
                        }
                    }
                }

                if(0 == click && 0 == detail && 0 == cart && 0 == cartDelete && 0 == buy && 0 == follow){
                    continue;
                }
                else {
                    hasAction = true;
                }
                log.info(date + ": user_id: " + userId + ", product_id: " + productId + ", click: " + click + ", detail: " + detail + ", cart: " + cart + ", cartDelete: " + cartDelete + ", buy: " + buy + ", follow: " + follow);

                LocalDate startDate = LocalDate.parse(date);
                int diff = dateSegment.get(startDate.toString());
                DayOfWeek week = startDate.getDayOfWeek();
                if(week == DayOfWeek.SATURDAY || week == DayOfWeek.SUNDAY){
                    insertMap.put("contains_weekend_" + diff, 1);
                }

                if(click > 0){
                    String key = "click_" + diff;
                    if(!actionAccu.containsKey(key)){
                        actionAccu.put(key, 0L);
                    }
                    actionAccu.put(key, actionAccu.get(key) + click);
                    String[] parts = modelId.split(",");
                    for(String part : parts){
                        if(!"0".equals(part)){
                            if(!map.containsKey(part)){
                                map.put(part, 0);
                            }

                            map.put(part, map.get(part) + 1);
                        }
                    }

                    for(Map.Entry<String, Integer> entry : map.entrySet()){
                        insertMap.put("model_" + entry.getKey(), entry.getValue());
                    }
                }

                if(detail > 0){
                    String key = "detail_" + diff;
                    if(!actionAccu.containsKey(key)){
                        actionAccu.put(key, 0L);
                    }
                    actionAccu.put(key, actionAccu.get(key) + detail);
                }

                if(cart > 0){
                    String key = "cart_" + diff;
                    if(!actionAccu.containsKey(key)){
                        actionAccu.put(key, 0L);
                    }
                    actionAccu.put(key, actionAccu.get(key) + cart);
                }

                if(cartDelete > 0){
                    String key = "cart_delete_" + diff;
                    if(!actionAccu.containsKey(key)){
                        actionAccu.put(key, 0L);
                    }
                    actionAccu.put(key, actionAccu.get(key) + cartDelete);
                }

                if(buy > 0){
                    String key = "buy_" + diff;
                    if(!actionAccu.containsKey(key)){
                        actionAccu.put(key, 0L);
                    }
                    actionAccu.put(key, actionAccu.get(key) + buy);
                }

                if(follow > 0){
                    String key = "follow_" + diff;
                    if(!actionAccu.containsKey(key)){
                        actionAccu.put(key, 0L);
                    }
                    actionAccu.put(key, actionAccu.get(key) + follow);
                }
            }

            if(!hasAction){
                continue;
            }

            insertMap.putAll(actionAccu);

            Map<String, Object> user = userInfo.get(userId);
            Map<String, Object> product = productInfo.get(productId);
            Map<String, Object> comment = commentInfo.get(productId);

            insertMap.put("user_id", userId);
            insertMap.put("sku_id", productId);
//            insertMap.put("model_id", modelIds);
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
                int badComment = 0;
                double badCommentRate = (double) comment.get("bad_comment_rate");
                if(badCommentRate >= 0 && badCommentRate < 0.1){
                    badComment = 1;
                }
                else if(badCommentRate >= 0.1 && badCommentRate < 0.2)
                {
                    badComment = 2;
                }
                else if(badCommentRate >= 0.2 && badCommentRate < 0.3)
                {
                    badComment = 3;
                }
                else if(badCommentRate >= 0.3 && badCommentRate < 0.4)
                {
                    badComment = 4;
                }
                else if(badCommentRate >= 0.4 && badCommentRate < 0.5)
                {
                    badComment = 5;
                }
                else if(badCommentRate >= 0.5 && badCommentRate < 0.6)
                {
                    badComment = 6;
                }
                else if(badCommentRate >= 0.6 && badCommentRate < 0.7)
                {
                    badComment = 7;
                }
                else if(badCommentRate >= 0.7 && badCommentRate < 0.8)
                {
                    badComment = 8;
                }
                else if(badCommentRate >= 0.8 && badCommentRate < 0.9)
                {
                    badComment = 9;
                }
                else if(badCommentRate >= 0.9 && badCommentRate <= 1.0)
                {
                    badComment = 10;
                }
                insertMap.put("bad_comment_rate", badComment);
                insertMap.put("last_comment_date", comment.get("last_date"));
            }
            else {
                insertMap.put("comment_num", 0);
                insertMap.put("has_bad_comment", 0);
                insertMap.put("bad_comment_rate", 10);
                insertMap.put("last_comment_date", null);
            }

            DBOperation.insert(conn, "user_action_segment", insertMap);
        }
	}
}


















