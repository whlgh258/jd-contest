package jd.contest;

import com.alibaba.fastjson.JSONObject;
import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.Date;
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
public class UserUpdateConsumer implements Runnable
{
	private static final Logger log = Logger.getLogger( UserUpdateConsumer.class );

	private BlockingQueue<Map<String, Object>> userQueue;
	private Connection conn;

	public UserUpdateConsumer(BlockingQueue<Map<String, Object>> userQueue, Connection conn )
	{
		this.userQueue = userQueue;
		this.conn = conn;
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
        Map<Integer, Integer> map = new HashMap<>();
        int userId = (int) row.get("user_id");
        int skuId = (int) row.get("sku_id");
        Date date = (Date) row.get("action_date");

        String actionSql = "select model_id from action_1 where user_id=" + userId + " and sku_id=" + skuId + " and date='" + date + "'";
        List<Map<String, Object>> actionResult = DBOperation.queryBySql(actionSql);
        for(Map<String, Object> actionRow : actionResult){
            int modelId = (int) actionRow.get("model_id");
            if(modelId > 0){
                if(!map.containsKey(modelId)){
                    map.put(modelId, 0);
                }

                map.put(modelId, map.get(modelId) + 1);
            }
        }

        if(map.size() > 0) {
            JSONObject json = new JSONObject();
            for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
                json.put(String.valueOf(entry.getKey()), entry.getValue());
            }

            String str = json.toJSONString();
            String updateSql = "update user_action_1 set model_id='" + str + "' where user_id=" + userId + " and sku_id=" + skuId + " and action_date='" + date + "'";
            log.info(updateSql);
            DBOperation.update(updateSql);
        }
	}
}


















