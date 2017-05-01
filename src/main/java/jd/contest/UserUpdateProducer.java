package jd.contest;

import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
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
public class UserUpdateProducer implements Runnable
{
	private static final Logger log = Logger.getLogger( UserUpdateProducer.class );

	private BlockingQueue<Map<String, Object>> userQueue;
	private List<Map<String, Object>> result;

	public UserUpdateProducer(BlockingQueue<Map<String, Object>> userQueue, List<Map<String, Object>> result)
	{
        this.userQueue = userQueue;
		this.result = result;
	}

	@Override
	public void run()
	{
		try
		{
			produceUser();
            userQueue.add(null);
		}
		catch ( Exception e )
		{
			log.error( e.getMessage() );
		}
	}
	
	private void produceUser()
	{
		int i = 0;
		for( Map<String, Object> row : result )
		{
			try
			{
				userQueue.put( row );
			}
			catch ( InterruptedException e )
			{
				log.error( e.getMessage(), e );
			}
		}
	}
}


















