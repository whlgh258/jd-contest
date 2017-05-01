package jingdong.contest.test;

import au.com.bytecode.opencsv.CSVWriter;
import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
public class UserConsumerTest implements Runnable
{
	private static final Logger log = Logger.getLogger( UserConsumerTest.class );
	
	private BlockingQueue<Integer> userQueue;

	private Connection conn;
    private CSVWriter writer;
    private List<String> columns;
	
	public UserConsumerTest(BlockingQueue<Integer> userQueue, Connection conn, CSVWriter writer, List<String> columns)
	{
		this.userQueue = userQueue;
		this.conn = conn;
        this.writer = writer;
        this.columns = columns;
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
	
	private void handle(int limit) {
        String sql = "select * from user_action_horizon limit " + limit + ",10000";
        log.info(sql);
        List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
        for (Map<String, Object> row : result) {
            int buy1 = (int) row.get("buy_1");
            int buy2 = (int) row.get("buy_2");
            int buy3 = (int) row.get("buy_3");
            int buy4 = (int) row.get("buy_4");
            int buy5 = (int) row.get("buy_5");

            /**
             * 正例
             * 4.11-4.15号购买过的且2.1-4.10有过行为的作为正例，2.1-4.10的行为作为feature
             */
            if (buy1 > 0 || buy2 > 0 || buy3 > 0 || buy4 > 0 || buy5 > 0) {
                boolean hasAction = false;
                for(int j = 6; j <= 75; j++){
                    if((int) row.get("click_" + j) > 0 ||
                            (int) row.get("detail_" + j) > 0 ||
                            (int) row.get("cart_" + j) > 0 ||
                            (int) row.get("cart_delete_" + j) > 0 ||
                            (int) row.get("follow_" + j) > 0){

                        hasAction = true;
                        if(hasAction){
                            break;
                        }
                    }
                }

                if(true){
                    removeKey(row, 5);
                    row.put("buy", 1);
                }
            }
            /**
             * 负例
             * 4.11-4.15号未购买过，但2.1-4.10号有过动作的作为负例
             * 正例1386,负例8倍 -> 11088
             * 是找已购买过的用户还是所有用户作为负例？
             *
             * 训练使用4.6-4.10号购买过的
             * 测试使用4.11-4.15号的数据
             *
             * 是否5天分为一个区间？
             *
             * model_id还需要按日期处理一下
             */
            else if (0 == buy1 && 0 == buy2 && 0 == buy3 && 0 == buy4 && 0 == buy5) {
                boolean hasAction = false;
                for(int j = 6; j <= 75; j++){
                    if((int) row.get("click_" + j) > 0 ||
                            (int) row.get("detail_" + j) > 0 ||
                            (int) row.get("cart_" + j) > 0 ||
                            (int) row.get("cart_delete_" + j) > 0 ||
                            (int) row.get("follow_" + j) > 0){

                        hasAction = true;
                        if(hasAction){
                            break;
                        }
                    }
                }

                if(hasAction){
                    removeKey(row, 5);
                    row.put("buy", 0);
                }
            }

            synchronized (this){
                List<String> elements = new ArrayList<>();
                for(String column : columns){
                    elements.add(String.valueOf(row.get(column)));
                }

                writer.writeNext(elements.toArray(new String[0]));
            }
        }
    }

    private void removeKey(Map<String, Object> row, int k){
        for (int j = 1; j <= k; j++) {
            row.remove("click_" + j);
            row.remove("detail_" + j);
            row.remove("cart_" + j);
            row.remove("cart_delete_" + j);
            row.remove("buy_" + j);
            row.remove("follow_" + j);
        }
    }
}


















