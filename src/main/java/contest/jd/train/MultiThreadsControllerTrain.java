package contest.jd.train;

import au.com.bytecode.opencsv.CSVWriter;
import multithreads.DBConnection;
import org.apache.log4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;


/**
 * 
 * Description: Initial work for program running
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
public class MultiThreadsControllerTrain
{
	private static final Logger log = Logger.getLogger( MultiThreadsControllerTrain.class );
	
	/**
	 * contoller function
	 */
	public void control()
	{
        log.info(Runtime.getRuntime().availableProcessors());
		Connection conn = DBConnection.getConnection();
        int queueSize = 10;
        int threadSize = 20;
        Connection[] connections = new Connection[threadSize];

		try
		{
			long CurrentTime = System.currentTimeMillis();

			BlockingQueue<Integer> userQueue = new ArrayBlockingQueue<>(queueSize);

            List<Integer> limits = new ArrayList<>();
            for(int i = 0; i < 1455980; i += 10000){
                limits.add(i);
            }

            Set<Map<String, Object>> rows = new HashSet<>();
            BlockingQueue<Map<String, Object>> positiveRows = new LinkedBlockingDeque<>();
            BlockingQueue<Map<String, Object>> falseRows = new LinkedBlockingDeque<>();


			//start producer
			UserProducerTrain producer = new UserProducerTrain(userQueue, limits);
			new Thread(producer).start();

            for(int i = 0; i < connections.length; i++){
                connections[i] = DBConnection.getConnection();
            }
			
			//start consumer
			Thread [] threads = new Thread[threadSize];
			for( int i = 0; i < threads.length; i++ )
			{
				threads[i] = new Thread(new UserConsumerTrain(userQueue, positiveRows, falseRows, connections[i]));
				threads[i].start();
			}
			
			for( int i = 0; i < threads.length; i++ )
			{
				threads[i].join();
			}

            log.info("positive: " + positiveRows.size());
            log.info("false: " + falseRows.size());

            rows.addAll(positiveRows);
            rows.addAll(falseRows);
            log.info("train: " + rows.size());
            writeToFile(rows);


			log.info( "The program spent time is: " + ( System.currentTimeMillis() - CurrentTime ) /60000 );
			log.info( "--------------------------------------run completely---------------------------------------------" );
		}
		catch ( Exception e )
		{
			log.error( e.getMessage(), e);
		}
		finally
		{
			DBConnection.close( conn );
            for(int i = 0; i < connections.length; i++){
                DBConnection.close(conn);
            }
		}
	}
	
	public static void main( String[] args )
	{
		MultiThreadsControllerTrain controller = new MultiThreadsControllerTrain();
		controller.control();
	}

    private void writeToFile(Set<Map<String, Object>> set){
        Map<String, Object>[] array = set.toArray(new Map[0]);
        Map<String, Object> row = array[0];
        Set<String> keys = row.keySet();
        List<String> columns = new ArrayList<>();
        columns.addAll(keys);

        try {
            CSVWriter writer = new CSVWriter( new FileWriter("/home/wanghl/jd_contest/0417/train.csv"), ',', '\0');
            writer.writeNext(columns.toArray(new String[0]));

            for(Map<String, Object> line : set){
                List<String> elements = new ArrayList<>();
                for(String column : columns){
                    elements.add(String.valueOf(line.get(column)));
                }

                writer.writeNext(elements.toArray(new String[0]));
            }

            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


















