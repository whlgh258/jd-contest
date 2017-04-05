import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * 配置属性解析类
 * 可以获取配置的int、String和boolean类型的属性
 * 如果某属性未配置，则使用默认值
 * @author wanghonglei
 *
 */
public final class Configurations
{
	private static final Logger log = Logger.getLogger( Configurations.class );
	
	private static Properties prop = new Properties();

	public static String getStringProperty( String key, String defaultValue )
	{
		if ( prop == null )
		{
			return defaultValue;
		}
		return prop.getProperty( key );
	}

	public static int getIntProperty( String key, int defaultValue )
	{
		if ( prop == null )
		{
			return defaultValue;
		}
		int value = defaultValue;
		try
		{
			value = Integer.parseInt( prop.getProperty( key ) );
		}
		catch ( NumberFormatException e )
		{
			return defaultValue;
		}
		return value;
	}

	public static long getLongProperty( String key, long defaultValue )
	{
		if ( prop == null )
		{
			return defaultValue;
		}
		long value = defaultValue;
		try
		{
			value = Long.parseLong( prop.getProperty( key ) );
		}
		catch ( NumberFormatException e )
		{
			return value;
		}
		return value;
	}

	public static boolean getBooleanProperty( String key, boolean defaultValue )
	{
		if ( prop == null )
		{
			return defaultValue;
		}
		return prop.getProperty( key ).toLowerCase().trim().equals( "true" );
	}
	
	public static List<String> getListProperties( String key, List<String> defaultList )
	{
		List<String> list = new ArrayList<String>();
		if( null == prop )
		{
			return defaultList;
		}
		
		String value = prop.getProperty( key );
		String[] values = value.split( "," );
		for( String v : values )
		{
			list.add( v );
		}
		
		return list;
	}
	
	public static double getDoubleProperty( String key, double defaultValue )
	{
		if ( prop == null )
		{
			return defaultValue;
		}
		double value = defaultValue;
		try
		{
			value = Double.parseDouble( prop.getProperty( key ) );
		}
		catch ( NumberFormatException e )
		{
			return value;
		}
		
		return value;
	}

	static
	{
		try
		{
			prop.load( Configurations.class.getClassLoader().getResourceAsStream( "yongche.properties" ) );
		}
		catch ( Exception e )
		{
			prop = null;
			log.info( "WARNING: Could not find yongche.properties file in class path. I will use the default values." );
			System.err.println( "WARNING: Could not find yongche.properties file in class path. I will use the default values." );
		}
	}
}
