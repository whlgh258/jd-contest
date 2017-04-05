import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

/**
 * 数据库连接类，初始化数据库连接池，并提供获取数据库连接
 * 和释放数据库连接的对外接口，数据库连接池使用了DBCP
 * 
 * @author wanghl9
 *
 */
public class DBConnection
{
	private final static Logger log = Logger.getLogger( DBConnection.class );
	private static BasicDataSource ds = new BasicDataSource();
	
	private DBConnection()
	{
	}

	//使用配置文件初始化连接池
	static
	{
		try
		{

			if ( log.isDebugEnabled() )
			{

				log.debug( "正在初始化数据连接池" );

			}

			Properties props = new Properties();
			props.load( DBConnection.class.getClassLoader().getResourceAsStream( "DBConfig.properties" ) );
			BeanUtils.populate( ds, props );

			if ( log.isDebugEnabled() )
			{
				log.debug( "数据连接池初始化完成" + ", 最大可用连接数：" + ds.getMaxActive() + ", 最小连接数："
				+ ds.getNumIdle() );
			}
		}
		catch ( IOException e )
		{
			log.error( "连接池初始化失败，原因：" + e.getMessage(), e );

		}
		catch ( IllegalAccessException e )
		{
			log.error( "连接池初始化失败，原因：" + e.getMessage(), e );

		}
		catch ( InvocationTargetException e )
		{
			log.error( "连接池初始化失败，原因：" + e.getMessage(), e );
		}
	}

	/**
	 * 获取数据库连接
	 * @return 数据库连接
	 */
	public static Connection getConnection()
	{
		try
		{
			return ds.getConnection();
		}
		catch ( SQLException e )
		{
			log.error( e.getMessage(), e );
		}
		return null;
	}
	
	/**
	 * 获取数据库连接
	 * @return 数据库连接
	 */
	public static Connection getConnection( String dburl, String username, String password )
	{
		try
		{
			return DriverManager.getConnection( dburl, username, password );
		}
		catch ( SQLException e )
		{
			log.error( e.getMessage(), e );
		}
		return null;
	}

	/**
	 * 关闭数据库连接
	 * @param conn
	 */
	public static void close( Connection conn )
	{
		try
		{
			conn.close();
		}
		catch ( Exception e )
		{
			log.error( e.getMessage(), e );
		}
	}

	/**
	 * 关闭数据库连接池
	 */
	public static void closeDataPool()
	{
		if ( null != ds )
		{
			try
			{
				ds.close();
			}
			catch ( SQLException e )
			{
				log.error( e.getMessage(), e );
			}
		}
	}

	public static void main( String[] args )
	{
		Connection conn = DBConnection.getConnection();

		System.out.println( conn );
	}
}
