package contest.jd.test;

import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * 
 * Description: puts domain into queue for consumer	
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
public class UserProducerTest implements Runnable
{
	private static final Logger log = Logger.getLogger( UserProducerTest.class );
	
	private BlockingQueue<Integer> userQueue;
	private List<Integer> userIds;

	public UserProducerTest(BlockingQueue<Integer> userQueue, List<Integer> userIds)
	{
        this.userQueue = userQueue;
		this.userIds = userIds;
	}

	@Override
	public void run()
	{
		try
		{
			produceUser();
            userQueue.put(Integer.MAX_VALUE);
		}
		catch ( Exception e )
		{
			log.error( e.getMessage(), e);
		}
	}
	
	private void produceUser()
	{
		int i = 0;
		for( int userId : userIds )
		{
			try
			{
				userQueue.put( userId );
			}
			catch ( InterruptedException e )
			{
				log.error( e.getMessage(), e );
			}
		}
	}
}


















