package contest.jd.test;

import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.sql.Connection;
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
    private BlockingQueue<Map<String, Object>> testRows;

    private static final Random random = new Random();

	private Connection conn;
	
	public UserConsumerTest(BlockingQueue<Integer> userQueue, BlockingQueue<Map<String, Object>> testRows, Connection conn )
	{
		this.userQueue = userQueue;
		this.testRows = testRows;
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
	
	private void handle(int limit) {
        String sql = "select * from user_action_segment limit " + limit + ",10000";
        log.info(sql);
        List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
        for (Map<String, Object> row : result) {
            try {
                int buy2 = (int) row.get("buy_2");
                if(buy2 > 0){
                    row.put("buy",1);
                }
                else {
                    row.put("buy", 0);
                }
                changeData(row);
                testRows.put(row);
            }
            catch (Exception e){
                log.error(e.getMessage(), e);
            }
        }
    }

	private void changeData(Map<String, Object> row){
        for(int i = 16; i > 3; i--){
            row.put("click_" + i, row.get("click_" + (i - 1)));
            row.put("detail_" + i, row.get("detail_" + (i - 1)));
            row.put("cart_" + i, row.get("cart_" + (i - 1)));
            row.put("cart_delete_" + i, row.get("cart_delete_" + (i - 1)));
            row.put("buy_" + i, row.get("buy_" + (i - 1)));
            row.put("follow_" + i, row.get("follow_" + (i - 1)));
        }

        for(int i = 1; i <=3; i++){
            row.remove("click_" + i);
            row.remove("detail_" + i);
            row.remove("cart_" + i);
            row.remove("cart_delete_" + i);
            row.remove("buy_" + i);
            row.remove("follow_" + i);
        }

        for(int i = 1; i <=16; i++){
            row.remove("contains_weekend_" + i);
        }
    }
}


















