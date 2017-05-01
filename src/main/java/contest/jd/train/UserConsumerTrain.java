package contest.jd.train;

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
public class UserConsumerTrain implements Runnable
{
	private static final Logger log = Logger.getLogger( UserConsumerTrain.class );
	
	private BlockingQueue<Integer> userQueue;
    private BlockingQueue<Map<String, Object>> positiveRows;
    private BlockingQueue<Map<String, Object>> falseRows;

    private static final Random random = new Random();

	private Connection conn;
	
	public UserConsumerTrain(BlockingQueue<Integer> userQueue, BlockingQueue<Map<String, Object>> positiveRows, BlockingQueue<Map<String, Object>> falseRows, Connection conn )
	{
		this.userQueue = userQueue;
		this.positiveRows = positiveRows;
        this.falseRows = falseRows;
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
            try{
                int buy3 = (int) row.get("buy_3");

                /**
                 * 正例
                 * 4.11-4.15号购买过的且2.1-4.10有过行为的作为正例，2.1-4.10的行为作为feature
                 */
                if (buy3 > 0) {
                    boolean hasAction = false;
                    for(int j = 4; j <= 16; j++){
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
                        removeKey(row, 3);
                        row.put("buy", 1);
                        positiveRows.put(row);
                    }
                }
                /**
                 * 负例
                 * 4.11-4.15号未购买过，但2.1-4.10号有过动作的作为负例
                 * 正例1717,负例8倍 -> 13736
                 * 是找已购买过的用户还是所有用户作为负例？
                 *
                 * 训练使用4.6-4.10号购买过的
                 * 测试使用4.11-4.15号的数据
                 *
                 * 是否5天分为一个区间？
                 *
                 * model_id还需要按日期处理一下
                 */
                else if (0 == buy3) {
                    boolean hasAction = false;
                    for(int j = 4; j <= 16; j++){
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
                        double rand = random.nextDouble();
                        if(rand > 0.5 && falseRows.size() < 13736){
                            removeKey(row, 3);
                            row.put("buy", 0);
                            falseRows.put(row);
                        }
                    }
                }
            }
            catch (Exception e){
                log.error(e.getMessage(), e);
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

            for(int i = 1; i <=16; i++){
                row.remove("contains_weekend_" + i);
            }
        }
    }
}


















