package multithreads;

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class DBOperation
{
	private final static Logger log = Logger.getLogger( DBConnection.class );
	private final static int commitSize = 100000;
	
	
	public static List<String> getTableNameList()
	{
		List<String> result = new ArrayList<String>();
		Connection conn = null;
		try
		{
			conn = DBConnection.getConnection();
			ResultSet rs = conn.getMetaData().getTables( null, null, null, null );
			ResultSetMetaData metaData = rs.getMetaData();
			while ( rs.next() )
			{
				String columName = metaData.getColumnLabel( 3 );
				Object obj = rs.getObject( columName );
				result.add( obj.toString() );
			}
		}
		catch ( Exception e )
		{
			log.error( e.getMessage(), e );
		}
		finally
		{
			if ( null != conn )
			{
				DBConnection.close( conn );
			}
		}
		return result;
	}

	@SuppressWarnings("resource")
	public static void createtable( String tableName, LinkedHashMap<String, String> fieldsNameStyle, String primaryKey, String others )
	{
		String sql = "";
		PreparedStatement pstmt = null;

		Set<String> AllFieldsName = fieldsNameStyle.keySet();

		Iterator<String> FieldNameIterator = AllFieldsName.iterator();

		String TmpSqlPart = "";

		while ( FieldNameIterator.hasNext() )
		{
			String FieldName = FieldNameIterator.next();
			String FieldStyle = fieldsNameStyle.get( FieldName );

			TmpSqlPart = TmpSqlPart + FieldName + " " + FieldStyle;
			TmpSqlPart += ",";
		}

		String SqlPart = TmpSqlPart + primaryKey;

		sql = "create table " + tableName + "(" + SqlPart + ")" + others;

		Connection conn = null;
		try
		{
			conn = DBConnection.getConnection();

			// 这里的操作为如果表已存在就先把该表删除
			String dropsql = "drop table IF EXISTS " + tableName;
			pstmt = conn.prepareStatement( dropsql );
			pstmt.executeUpdate();
			// /////////////////////////////////////////////////

			// 下面的操作为若表已存在就返回，不做处理
			// ResultSet rs = conn.getMetaData().getTables( null, null,
			// TableName, null );
			// if( rs.next())
			// {
			// return;
			// }
			// ////////////////////////////////////////////////////////

			pstmt = conn.prepareStatement( sql );

			pstmt.executeUpdate();
		}
		catch ( Exception e )
		{
			log.error( sql );
			log.error( e.getMessage(), e );

		}
		finally
		{
			try
			{
				if( null != pstmt )
				{
					pstmt.close();
				}
				if ( null != conn )
				{
					DBConnection.close( conn );
				}
			}
			catch ( SQLException e )
			{
				e.printStackTrace();
			}
		}

	}

	// 重载createtable方法
	@SuppressWarnings("resource")
	public static void createtable( String tableName, String sql )
	{
		if ( null != sql )
		{
			PreparedStatement pstmt = null;
			Connection conn = null;
			try
			{
				conn = DBConnection.getConnection();

				String dropsql = "drop table IF EXISTS " + tableName;
				pstmt = conn.prepareStatement( dropsql );
				pstmt.executeUpdate();

				pstmt = conn.prepareStatement( sql );

				pstmt.executeUpdate();
			}
			catch ( Exception e )
			{
				log.error( sql );
				log.error( e.getMessage(), e );

			}
			finally
			{
				try
				{
					if( null != pstmt )
					{
						pstmt.close();
					}
					if ( null != conn )
					{
						DBConnection.close( conn );
					}
				}
				catch ( SQLException e )
				{
					e.printStackTrace();
				}
			}
		}
		else
		{
			System.out.println( "sql语句为空" );
		}
	}

	public static void dropTable( String tableName )
	{
		PreparedStatement pstmt = null;
		Connection conn = null;
		String sql = null;
		try
		{
			conn = DBConnection.getConnection();

			sql = "drop table IF EXISTS " + tableName;
			pstmt = conn.prepareStatement( sql );
			pstmt.executeUpdate();
		}
		catch ( Exception e )
		{
			log.error( sql );
			log.error( e.getMessage(), e );
		}
		finally
		{
			try
			{
				if( null != pstmt )
				{
					pstmt.close();
				}
				if ( null != conn )
				{
					DBConnection.close( conn );
				}
			}
			catch ( SQLException e )
			{
				e.printStackTrace();
			}
		}
	}

	public static void insert( String tableName, Map<String, Object> fieldsNameValue )
	{
		String sql = "";
		PreparedStatement pstmt = null;

		// 取出字段的名称
		Set<String> AllFieldName = fieldsNameValue.keySet();

		Iterator<String> FieldNameIterator = AllFieldName.iterator();

		// n为字段个数
		int n = fieldsNameValue.size();
		String TmpPreSqlPart = "";
		String RearSqlPart = "?";

		while ( FieldNameIterator.hasNext() )
		{
			TmpPreSqlPart += FieldNameIterator.next();
			TmpPreSqlPart += ",";
		}

		int LastIndex = TmpPreSqlPart.lastIndexOf( ',' );
		String PreSqlPart = TmpPreSqlPart.substring( 0, LastIndex );

		for ( int k = 0; k < n - 1; k++ )
		{
			RearSqlPart += ", ?";
		}

		// 组装sql语句
		sql = "insert into " + tableName + "(" + PreSqlPart + ")" + " values" + "(" + RearSqlPart + ")";

		Connection conn = null;
		try
		{
			conn = DBConnection.getConnection();
			// System.out.println( conn );
			pstmt = conn.prepareStatement( sql );

			FieldNameIterator = AllFieldName.iterator();

			// 若做成通用接口的话，可以对PrepareStatement中所有set方法的参数类型进行判断
			// 不过双方协商好要插入数据库的数据类型，只进行几种判断即可
			String FieldsName;
			Object object;
			int i = 1;
			while ( FieldNameIterator.hasNext() )
			{
				FieldsName = FieldNameIterator.next();
				object = fieldsNameValue.get( FieldsName );

				if ( object == null )
				{
					String string = "";
					pstmt.setString( i, string );
				}
				else if ( object instanceof Integer )
				{
					int value = ( (Integer) ( (Integer) object ) ).intValue();
					pstmt.setInt( i, value );
				}

				else if ( object instanceof String )
				{
					String string = (String) object;
					pstmt.setString( i, string );
				}
				else if ( object instanceof Double )
				{
					double doublevalue = ( (Double) object ).doubleValue();
					pstmt.setDouble( i, doublevalue );
				}
				else if ( object instanceof Float )
				{
					float floatvalue = ( (Float) object ).floatValue();
					pstmt.setFloat( i, floatvalue );
				}
				else if ( object instanceof Long )
				{
					long longvalue = ( (Long) object ).longValue();
					pstmt.setLong( i, longvalue );
				}
				else if ( object instanceof Boolean )
				{
					boolean booleanvalue = ( (Boolean) object ).booleanValue();
					pstmt.setBoolean( i, booleanvalue );
				}
				else if ( object instanceof Date )
				{
					Date date = (Date) object;
					pstmt.setDate( i, date );
				}
				else if( object instanceof InputStream )
				{
					InputStream is = ( InputStream )object;
					pstmt.setBlob( i, is );
				}
				else if( object instanceof Timestamp )
				{
					Timestamp timestamp = ( Timestamp )object;
					pstmt.setTimestamp( i, timestamp );
				}

				i++;
			}

			pstmt.executeUpdate();
		}
		catch ( Exception e )
		{
			log.error( sql );
			log.error( e.getMessage(), e );
		}
		finally
		{
			try
			{
				if( null != pstmt )
				{
					pstmt.close();
				}
				if ( null != conn )
				{
					DBConnection.close( conn );
				}
			}
			catch ( SQLException e )
			{
				e.printStackTrace();
			}
		}
	}

	public static void insert( String tableName, List<Map<String, Object>> allRows )
	{
		Connection conn = DBConnection.getConnection();
		
		try
		{
			conn.setAutoCommit( false );
		}
		catch ( SQLException e1 )
		{
			log.error( e1.getMessage(), e1 );
		}
		
		String sql = "";
		PreparedStatement pstmt = null;
		Map<String, Object> AllFieldsNameValue = null;
		Set<String> AllFieldName = null;
		Iterator<String> FieldNameIterator;

		if ( allRows.size() > 0 )
		{
			AllFieldsNameValue = allRows.get( 0 );

			// 取出字段的名称
			AllFieldName = AllFieldsNameValue.keySet();

			FieldNameIterator = AllFieldName.iterator();

			int n = AllFieldsNameValue.size();
			String TmpPreSqlPart = "";
			String RearSqlPart = "?";

			while ( FieldNameIterator.hasNext() )
			{
				TmpPreSqlPart += FieldNameIterator.next();
				TmpPreSqlPart += ",";
			}

			int LastIndex = TmpPreSqlPart.lastIndexOf( ',' );
			String PreSqlPart = TmpPreSqlPart.substring( 0, LastIndex );

			for ( int k = 0; k < n - 1; k++ )
			{
				RearSqlPart += ", ?";
			}

			// 组装sql语句
			sql = "insert into " + tableName + "(" + PreSqlPart + ")" + " values" + "(" + RearSqlPart + ")";
		}

		try
		{
			pstmt = conn.prepareStatement( sql );
			
			int rowSize = allRows.size();
			int batchCommitSize = 0;
			for ( int j = 0; j < rowSize; j ++ )
			{
				AllFieldsNameValue = allRows.get( j );
				AllFieldName = AllFieldsNameValue.keySet();
				FieldNameIterator = AllFieldName.iterator();

				// System.out.println( conn );

				// 若做成通用接口的话，可以对PrepareStatement中所有set方法的参数类型进行判断
				// 不过双方协商好要插入数据库的数据类型，只进行几种判断即可
				String FieldsName = "";
				Object object;
				int i = 1;
				while ( FieldNameIterator.hasNext() )
				{
					FieldsName = FieldNameIterator.next();
					object = AllFieldsNameValue.get( FieldsName );

					if ( object == null )
					{
						pstmt.setString( i, null );
					}
					else if ( object instanceof Integer )
					{
						int value = ( (Integer) ( (Integer) object ) ).intValue();
						pstmt.setInt( i, value );
					}

					else if ( object instanceof String )
					{
						String string = (String) object;
						pstmt.setString( i, string );
					}
					else if ( object instanceof Double )
					{
						double doublevalue = ( (Double) object ).doubleValue();
						pstmt.setDouble( i, doublevalue );
					}
					else if ( object instanceof Float )
					{
						float floatvalue = ( (Float) object ).floatValue();
						pstmt.setFloat( i, floatvalue );
					}
					else if ( object instanceof Long )
					{
						long longvalue = ( (Long) object ).longValue();
						pstmt.setLong( i, longvalue );
					}
					else if ( object instanceof Boolean )
					{
						boolean booleanvalue = ( (Boolean) object ).booleanValue();
						pstmt.setBoolean( i, booleanvalue );
					}
					else if ( object instanceof Date )
					{
						Date date = (Date) object;
						pstmt.setDate( i, date );
					}
					else if( object instanceof Timestamp )
					{
						Timestamp timestamp = ( Timestamp )object;
						pstmt.setTimestamp( i, timestamp );
					}

					i++;
				}
				
				pstmt.addBatch();
				if( ( j + 1 ) % commitSize == 0 )
				{
					int[] batchCommit = pstmt.executeBatch();
					batchCommitSize += batchCommit.length;
					conn.commit(); 
					pstmt.clearBatch();
				}
			}
			int[] batchCommit = pstmt.executeBatch();
			log.info( "batch commit: " + ( batchCommitSize + batchCommit.length ));
			conn.commit(); 
			pstmt.clearBatch();
		}
		catch ( Exception e )
		{
			log.error( sql );
			log.error( e.getMessage(), e );
		}
		finally
		{
			try
			{
				conn.setAutoCommit( true );
			}
			catch ( SQLException e1 )
			{
				log.error( e1.getMessage(), e1 );
			}
			try
			{
				if( null != pstmt )
				{
					pstmt.close();
					conn.close();
				}
			}
			catch ( SQLException e )
			{
				e.printStackTrace();
			}
		}
	}
	
	public static void insert( Connection conn, String tableName, List<Map<String, Object>> allRows )
	{
		try
		{
			conn.setAutoCommit( false );
		}
		catch ( SQLException e1 )
		{
			log.error( e1.getMessage(), e1 );
		}
		
		String sql = "";
		PreparedStatement pstmt = null;
		Map<String, Object> AllFieldsNameValue = null;
		Set<String> AllFieldName = null;
		Iterator<String> FieldNameIterator;

		if ( allRows.size() > 0 )
		{
			AllFieldsNameValue = allRows.get( 0 );

			// 取出字段的名称
			AllFieldName = AllFieldsNameValue.keySet();

			FieldNameIterator = AllFieldName.iterator();

			int n = AllFieldsNameValue.size();
			String TmpPreSqlPart = "";
			String RearSqlPart = "?";

			while ( FieldNameIterator.hasNext() )
			{
				TmpPreSqlPart += FieldNameIterator.next();
				TmpPreSqlPart += ",";
			}

			int LastIndex = TmpPreSqlPart.lastIndexOf( ',' );
			String PreSqlPart = TmpPreSqlPart.substring( 0, LastIndex );

			for ( int k = 0; k < n - 1; k++ )
			{
				RearSqlPart += ", ?";
			}

			// 组装sql语句
			sql = "insert into " + tableName + "(" + PreSqlPart + ")" + " values" + "(" + RearSqlPart + ")";
		}

		try
		{
			pstmt = conn.prepareStatement( sql );
			
			int rowSize = allRows.size();
			int batchCommitSize = 0;
			for ( int j = 0; j < rowSize; j ++ )
			{
				AllFieldsNameValue = allRows.get( j );
				AllFieldName = AllFieldsNameValue.keySet();
				FieldNameIterator = AllFieldName.iterator();

				// System.out.println( conn );

				// 若做成通用接口的话，可以对PrepareStatement中所有set方法的参数类型进行判断
				// 不过双方协商好要插入数据库的数据类型，只进行几种判断即可
				String FieldsName = "";
				Object object;
				int i = 1;
				while ( FieldNameIterator.hasNext() )
				{
					FieldsName = FieldNameIterator.next();
					object = AllFieldsNameValue.get( FieldsName );

					if ( object == null )
					{
						pstmt.setString( i, null );
					}
					else if ( object instanceof Integer )
					{
						int value = ( (Integer) ( (Integer) object ) ).intValue();
						pstmt.setInt( i, value );
					}

					else if ( object instanceof String )
					{
						String string = (String) object;
						pstmt.setString( i, string );
					}
					else if ( object instanceof Double )
					{
						double doublevalue = ( (Double) object ).doubleValue();
						pstmt.setDouble( i, doublevalue );
					}
					else if ( object instanceof Float )
					{
						float floatvalue = ( (Float) object ).floatValue();
						pstmt.setFloat( i, floatvalue );
					}
					else if ( object instanceof Long )
					{
						long longvalue = ( (Long) object ).longValue();
						pstmt.setLong( i, longvalue );
					}
					else if ( object instanceof Boolean )
					{
						boolean booleanvalue = ( (Boolean) object ).booleanValue();
						pstmt.setBoolean( i, booleanvalue );
					}
					else if ( object instanceof Date )
					{
						Date date = (Date) object;
						pstmt.setDate( i, date );
					}
					else if( object instanceof Timestamp )
					{
						Timestamp timestamp = ( Timestamp )object;
						pstmt.setTimestamp( i, timestamp );
					}

					i++;
				}
				
				pstmt.addBatch();
				if( ( j + 1 ) % commitSize == 0 )
				{
					int[] batchCommit = pstmt.executeBatch();
					log.info("batch commit: " + batchCommit.length);
					batchCommitSize += batchCommit.length;
					conn.commit(); 
					pstmt.clearBatch();
				}
			}
			int[] batchCommit = pstmt.executeBatch();
			log.info( "total batch commit: " + ( batchCommitSize + batchCommit.length ));
			conn.commit(); 
			pstmt.clearBatch();
		}
		catch ( Exception e )
		{
			log.error( sql );
			log.error( e.getMessage(), e );
		}
		finally
		{
			try
			{
				conn.setAutoCommit( true );
			}
			catch ( SQLException e1 )
			{
				log.error( e1.getMessage(), e1 );
			}
			try
			{
				if( null != pstmt )
				{
					pstmt.close();
				}
			}
			catch ( SQLException e )
			{
				e.printStackTrace();
			}
		}
	}

	public static void insert( Connection conn, String tableName, Map<String, Object> fieldsNameValue )
	{
		String sql = "";
		PreparedStatement pstmt = null;

		// 取出字段的名称
		Set<String> AllFieldName = fieldsNameValue.keySet();

		Iterator<String> FieldNameIterator = AllFieldName.iterator();

		int n = fieldsNameValue.size();
		String TmpPreSqlPart = "";
		String RearSqlPart = "?";

		while ( FieldNameIterator.hasNext() )
		{
			TmpPreSqlPart += FieldNameIterator.next();
			TmpPreSqlPart += ",";
		}

		int LastIndex = TmpPreSqlPart.lastIndexOf( ',' );
		String PreSqlPart = TmpPreSqlPart.substring( 0, LastIndex );

		for ( int k = 0; k < n - 1; k++ )
		{
			RearSqlPart += ", ?";
		}

		// 组装sql语句
		sql = "insert into " + tableName + "(" + PreSqlPart + ")" + " values" + "(" + RearSqlPart + ")";

		try
		{
			// System.out.println( conn );
			pstmt = conn.prepareStatement( sql );

			FieldNameIterator = AllFieldName.iterator();

			// 若做成通用接口的话，可以对PrepareStatement中所有set方法的参数类型进行判断
			// 不过双方协商好要插入数据库的数据类型，只进行几种判断即可
			String FieldsName;
			Object object;
			int i = 1;
			while ( FieldNameIterator.hasNext() )
			{
				FieldsName = FieldNameIterator.next();
				object = fieldsNameValue.get( FieldsName );

				if ( object == null )
				{
					String string = null;
					pstmt.setString( i, string );
				}
				else if ( object instanceof Integer )
				{
					int value = ( (Integer) ( (Integer) object ) ).intValue();
					pstmt.setInt( i, value );
				}

				else if ( object instanceof String )
				{
					String string = (String) object;
					pstmt.setString( i, string );
				}
				else if ( object instanceof Double )
				{
					double doublevalue = ( (Double) object ).doubleValue();
					pstmt.setDouble( i, doublevalue );
				}
				else if ( object instanceof Float )
				{
					float floatvalue = ( (Float) object ).floatValue();
					pstmt.setFloat( i, floatvalue );
				}
				else if ( object instanceof Long )
				{
					long longvalue = ( (Long) object ).longValue();
					pstmt.setLong( i, longvalue );
				}
				else if ( object instanceof Boolean )
				{
					boolean booleanvalue = ( (Boolean) object ).booleanValue();
					pstmt.setBoolean( i, booleanvalue );
				}
				else if ( object instanceof Date )
				{
					Date date = (Date) object;
					pstmt.setDate( i, date );
				}
				else if( object instanceof Timestamp )
				{
					Timestamp timestamp = ( Timestamp )object;
					pstmt.setTimestamp( i, timestamp );
				}

				i++;
			}

			pstmt.executeUpdate();
		}
		catch ( Exception e )
		{
			log.error( sql );
			log.error( e.getMessage(), e );
		}
		finally
		{
			// 在调用函数中关闭连接
			// DBConnection.close();
			try
			{
				if( null != pstmt )
				{
					pstmt.close();
				}
			}
			catch ( SQLException e )
			{
				e.printStackTrace();
			}
		}
	}

	// 重载insert方法
	public static void insert( String sql )
	{
		if ( null != sql )
		{
			PreparedStatement pstmt = null;
			Connection conn = null;
			try
			{
				conn = DBConnection.getConnection();
				pstmt = conn.prepareStatement( sql );

				pstmt.executeUpdate();
			}
			catch ( Exception e )
			{
				log.error( sql );
				log.error( e.getMessage(), e );
			}
			finally
			{
				try
				{
					if( null != pstmt )
					{
						pstmt.close();
					}
					if ( null != conn )
					{
						DBConnection.close( conn );
					}
				}
				catch ( SQLException e )
				{
					e.printStackTrace();
				}
			}
		}
		else
		{
			System.out.println( "sql语句为空" );
		}
	}

	// 重载insert方法
	public static void insert( Connection conn, String sql )
	{
		if ( null != sql )
		{
			PreparedStatement pstmt = null;

			try
			{
				pstmt = conn.prepareStatement( sql );

				pstmt.executeUpdate();
			}
			catch ( Exception e )
			{
				log.error( sql );
//				log.error( e.getMessage(), e );
			}
			finally
			{
				// 在调用函数中关闭连接
				// DBConnection.close();
				try
				{
					if( null != pstmt )
					{
						pstmt.close();
					}
				}
				catch ( SQLException e )
				{
					e.printStackTrace();
				}
			}
		}
		else
		{
			System.out.println( "sql语句为空" );
		}
	}

	// 更新操作
	public static void update( String tableName, Map<String, Object> fieldsNameValue, Map<String, Object> condNameValue )
	{
		String sql = "";
		PreparedStatement pstmt = null;

		Set<String> FieldsName = fieldsNameValue.keySet();
		Set<String> CondName = condNameValue.keySet();

		Iterator<String> FieldsIterator = FieldsName.iterator();
		Iterator<String> CondIterator = CondName.iterator();

		String PreSqlPart = "";
		while ( FieldsIterator.hasNext() )
		{
			PreSqlPart += FieldsIterator.next();
			PreSqlPart += "=? ";
			PreSqlPart += ",";
		}

		int LastIndex = PreSqlPart.lastIndexOf( ',' );
		PreSqlPart = PreSqlPart.substring( 0, LastIndex );

		String RearSqlPart = "";
		while ( CondIterator.hasNext() )
		{
			RearSqlPart += CondIterator.next();
			RearSqlPart += " = ?";
			RearSqlPart += " and ";
		}

		LastIndex = RearSqlPart.lastIndexOf( " and " );
		RearSqlPart = RearSqlPart.substring( 0, LastIndex );

		sql = "update " + tableName + " set " + PreSqlPart + " where " + RearSqlPart;

		Connection conn = null;
		try
		{
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement( sql );

			FieldsIterator = FieldsName.iterator();
			CondIterator = CondName.iterator();
			int i = 1;

			while ( FieldsIterator.hasNext() )
			{
				Object object = fieldsNameValue.get( FieldsIterator.next() );

				if ( object instanceof Integer )
				{
					int value = ( (Integer) ( (Integer) object ) ).intValue();
					pstmt.setInt( i, value );
				}
				else if ( object instanceof String )
				{
					String string = (String) object;
					pstmt.setString( i, string );
				}
				else if ( object instanceof Double )
				{
					double doublevalue = ( (Double) object ).doubleValue();
					pstmt.setDouble( i, doublevalue );
				}
				else if ( object instanceof Float )
				{
					float floatvalue = ( (Float) object ).floatValue();
					pstmt.setFloat( i, floatvalue );
				}
				else if ( object instanceof Long )
				{
					long longvalue = ( (Long) object ).longValue();
					pstmt.setLong( i, longvalue );
				}
				else if ( object instanceof Boolean )
				{
					boolean booleanvalue = ( (Boolean) object ).booleanValue();
					pstmt.setBoolean( i, booleanvalue );
				}
				else if ( object instanceof Date )
				{
					Date date = (Date) object;
					pstmt.setDate( i, date );
				}
				else if( object instanceof Timestamp )
				{
					Timestamp timestamp = ( Timestamp )object;
					pstmt.setTimestamp( i, timestamp );
				}

				i++;
			}

			while ( CondIterator.hasNext() )
			{
				Object object = condNameValue.get( CondIterator.next() );

				if ( object instanceof Integer )
				{
					int value = ( (Integer) ( (Integer) object ) ).intValue();
					pstmt.setInt( i, value );
				}
				else if ( object instanceof String )
				{
					String string = (String) object;
					pstmt.setString( i, string );
				}
				else if ( object instanceof Double )
				{
					double doublevalue = ( (Double) object ).doubleValue();
					pstmt.setDouble( i, doublevalue );
				}
				else if ( object instanceof Float )
				{
					float floatvalue = ( (Float) object ).floatValue();
					pstmt.setFloat( i, floatvalue );
				}
				else if ( object instanceof Long )
				{
					long longvalue = ( (Long) object ).longValue();
					pstmt.setLong( i, longvalue );
				}
				else if ( object instanceof Boolean )
				{
					boolean booleanvalue = ( (Boolean) object ).booleanValue();
					pstmt.setBoolean( i, booleanvalue );
				}
				else if ( object instanceof Date )
				{
					Date date = (Date) object;
					pstmt.setDate( i, date );
				}
				else if( object instanceof Timestamp )
				{
					Timestamp timestamp = ( Timestamp )object;
					pstmt.setTimestamp( i, timestamp );
				}

				i++;
			}

			pstmt.executeUpdate();
		}
		catch ( Exception e )
		{
			log.error( sql );
			log.error( e.getMessage(), e );
		}
		finally
		{
			try
			{
				if( null != pstmt )
				{
					pstmt.close();
				}
				if ( null != conn )
				{
					DBConnection.close( conn );
				}
			}
			catch ( SQLException e )
			{
				e.printStackTrace();
			}
		}
	}
	
	// 更新操作
		public static void update( Connection conn, String tableName, Map<String, Object> fieldsNameValue, Map<String, Object> condNameValue )
		{
			String sql = "";
			PreparedStatement pstmt = null;

			Set<String> FieldsName = fieldsNameValue.keySet();
			Set<String> CondName = condNameValue.keySet();

			Iterator<String> FieldsIterator = FieldsName.iterator();
			Iterator<String> CondIterator = CondName.iterator();

			String PreSqlPart = "";
			while ( FieldsIterator.hasNext() )
			{
				PreSqlPart += FieldsIterator.next();
				PreSqlPart += "=? ";
				PreSqlPart += ",";
			}

			int LastIndex = PreSqlPart.lastIndexOf( ',' );
			PreSqlPart = PreSqlPart.substring( 0, LastIndex );

			String RearSqlPart = "";
			while ( CondIterator.hasNext() )
			{
				RearSqlPart += CondIterator.next();
				RearSqlPart += " = ?";
				RearSqlPart += " and ";
			}

			LastIndex = RearSqlPart.lastIndexOf( " and " );
			RearSqlPart = RearSqlPart.substring( 0, LastIndex );

			sql = "update " + tableName + " set " + PreSqlPart + " where " + RearSqlPart;

			try
			{
				pstmt = conn.prepareStatement( sql );

				FieldsIterator = FieldsName.iterator();
				CondIterator = CondName.iterator();
				int i = 1;

				while ( FieldsIterator.hasNext() )
				{
					Object object = fieldsNameValue.get( FieldsIterator.next() );

					if ( object instanceof Integer )
					{
						int value = ( (Integer) ( (Integer) object ) ).intValue();
						pstmt.setInt( i, value );
					}
					else if ( object instanceof String )
					{
						String string = (String) object;
						pstmt.setString( i, string );
					}
					else if ( object instanceof Double )
					{
						double doublevalue = ( (Double) object ).doubleValue();
						pstmt.setDouble( i, doublevalue );
					}
					else if ( object instanceof Float )
					{
						float floatvalue = ( (Float) object ).floatValue();
						pstmt.setFloat( i, floatvalue );
					}
					else if ( object instanceof Long )
					{
						long longvalue = ( (Long) object ).longValue();
						pstmt.setLong( i, longvalue );
					}
					else if ( object instanceof Boolean )
					{
						boolean booleanvalue = ( (Boolean) object ).booleanValue();
						pstmt.setBoolean( i, booleanvalue );
					}
					else if ( object instanceof Date )
					{
						Date date = (Date) object;
						pstmt.setDate( i, date );
					}
					else if( object instanceof Timestamp )
					{
						Timestamp timestamp = ( Timestamp )object;
						pstmt.setTimestamp( i, timestamp );
					}

					i++;
				}

				while ( CondIterator.hasNext() )
				{
					Object object = condNameValue.get( CondIterator.next() );

					if ( object instanceof Integer )
					{
						int value = ( (Integer) ( (Integer) object ) ).intValue();
						pstmt.setInt( i, value );
					}
					else if ( object instanceof String )
					{
						String string = (String) object;
						pstmt.setString( i, string );
					}
					else if ( object instanceof Double )
					{
						double doublevalue = ( (Double) object ).doubleValue();
						pstmt.setDouble( i, doublevalue );
					}
					else if ( object instanceof Float )
					{
						float floatvalue = ( (Float) object ).floatValue();
						pstmt.setFloat( i, floatvalue );
					}
					else if ( object instanceof Long )
					{
						long longvalue = ( (Long) object ).longValue();
						pstmt.setLong( i, longvalue );
					}
					else if ( object instanceof Boolean )
					{
						boolean booleanvalue = ( (Boolean) object ).booleanValue();
						pstmt.setBoolean( i, booleanvalue );
					}
					else if ( object instanceof Date )
					{
						Date date = (Date) object;
						pstmt.setDate( i, date );
					}
					else if( object instanceof Timestamp )
					{
						Timestamp timestamp = ( Timestamp )object;
						pstmt.setTimestamp( i, timestamp );
					}

					i++;
				}

				pstmt.executeUpdate();
			}
			catch ( Exception e )
			{
				log.error( sql );
				log.error( e.getMessage(), e );
			}
			finally
			{
				try
				{
					if( null != pstmt )
					{
						pstmt.close();
					}
				}
				catch ( SQLException e )
				{
					e.printStackTrace();
				}
			}
		}

	// 重载update方法
	public static void update( String sql )
	{
		if ( null != sql )
		{
			PreparedStatement pstmt = null;
			Connection conn = null;
			try
			{
				conn = DBConnection.getConnection();

				pstmt = conn.prepareStatement( sql );

				pstmt.executeUpdate();
//				pstmt.execute();
			}
			catch ( Exception e )
			{
				log.error( sql );
				log.error( e.getMessage(), e );
			}
			finally
			{
				try
				{
					if( null != pstmt )
					{
						pstmt.close();
					}
					if ( null != conn )
					{
						DBConnection.close( conn );
					}
				}
				catch ( SQLException e )
				{
					e.printStackTrace();
				}
			}
		}
		else
		{
			System.out.println( "sql语句为空" );
		}
	}
	
	// 重载update方法
		public static void update( Connection conn, String sql )
		{
			if ( null != sql )
			{
				PreparedStatement pstmt = null;
				try
				{
					pstmt = conn.prepareStatement( sql );

					pstmt.executeUpdate();
				}
				catch ( Exception e )
				{
					log.error( sql );
					log.error( e.getMessage(), e );
				}
				finally
				{
					try
					{
						if( null != pstmt )
						{
							pstmt.close();
						}
					}
					catch ( SQLException e )
					{
						e.printStackTrace();
					}
				}
			}
			else
			{
				System.out.println( "sql语句为空" );
			}
		}

	// 删除操作
	public static void delete( String tableName, Map<String, Object> fieldsNameValue )
	{
		String sql = "";
		PreparedStatement pstmt = null;

		// 接收的参数：第一个是要操作的数据库表的名字；第二个是一个map，里面放着是要操作的字段名称
		// 和字段的值
		Map<String, Object> AllFieldsNameValue = fieldsNameValue;

		// 取出字段的名称
		Set<String> AllFieldName = AllFieldsNameValue.keySet();

		Iterator<String> FieldNameIterator = AllFieldName.iterator();
		String RearSqlPart = "";

		while ( FieldNameIterator.hasNext() )
		{
			RearSqlPart += FieldNameIterator.next();
			RearSqlPart += "=?";
			RearSqlPart += " and ";
		}

		int LastIndex = RearSqlPart.lastIndexOf( " and " );
		RearSqlPart = RearSqlPart.substring( 0, LastIndex );

		sql = "delete from " + tableName + " where " + RearSqlPart;
		Connection conn = null;
		try
		{
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement( sql );

			FieldNameIterator = AllFieldName.iterator();

			// 若做成通用接口的话，可以对PrepareStatement中所有set方法的参数类型进行判断
			// 不过双方协商好要插入数据库的数据类型，只进行几种判断即可
			int i = 1;
			while ( FieldNameIterator.hasNext() )
			{
				Object object = AllFieldsNameValue.get( FieldNameIterator.next() );

				if ( object instanceof Integer )
				{
					int value = ( (Integer) ( (Integer) object ) ).intValue();
					pstmt.setInt( i, value );
				}
				else if ( object instanceof String )
				{
					String string = (String) object;
					pstmt.setString( i, string );
				}
				else if ( object instanceof Double )
				{
					double doublevalue = ( (Double) object ).doubleValue();
					pstmt.setDouble( i, doublevalue );
				}
				else if ( object instanceof Float )
				{
					float floatvalue = ( (Float) object ).floatValue();
					pstmt.setFloat( i, floatvalue );
				}
				else if ( object instanceof Long )
				{
					long longvalue = ( (Long) object ).longValue();
					pstmt.setLong( i, longvalue );
				}
				else if ( object instanceof Boolean )
				{
					boolean booleanvalue = ( (Boolean) object ).booleanValue();
					pstmt.setBoolean( i, booleanvalue );
				}
				else if ( object instanceof Date )
				{
					Date date = (Date) object;
					pstmt.setDate( i, date );
				}

				i++;
			}

			pstmt.executeUpdate();
		}
		catch ( Exception e )
		{
			log.error( sql );
			log.error( e.getMessage(), e );
		}
		finally
		{
			try
			{
				if( null != pstmt )
				{
					pstmt.close();
				}
				if ( null != conn )
				{
					DBConnection.close( conn );
				}
			}
			catch ( SQLException e )
			{
				e.printStackTrace();
			}
		}
	}

	// 重载delete方法
	public static int delete( String sql )
	{
		int rows = 0;
		
		if ( null != sql )
		{
			PreparedStatement pstmt = null;
			Connection conn = null;
			try
			{
				conn = DBConnection.getConnection();

				pstmt = conn.prepareStatement( sql );

				rows = pstmt.executeUpdate();
			}
			catch ( Exception e )
			{
				log.error( sql );
				log.error( e.getMessage(), e );
			}
			finally
			{
				try
				{
					if( null != pstmt )
					{
						pstmt.close();
					}
					if ( null != conn )
					{
						DBConnection.close( conn );
					}
				}
				catch ( SQLException e )
				{
					e.printStackTrace();
				}
			}
		}
		else
		{
			System.out.println( "sql语句为空" );
		}
		
		return rows;
	}
	
	// 重载delete方法
		public static int delete( Connection conn, String sql )
		{
			int rows = 0;
			
			if ( null != sql )
			{
				PreparedStatement pstmt = null;
				try
				{
					pstmt = conn.prepareStatement( sql );

					rows = pstmt.executeUpdate();
				}
				catch ( Exception e )
				{
					log.error( sql );
					log.error( e.getMessage(), e );
				}
				finally
				{
					try
					{
						if( null != pstmt )
						{
							pstmt.close();
						}
					}
					catch ( SQLException e )
					{
						e.printStackTrace();
					}
				}
			}
			else
			{
				System.out.println( "sql语句为空" );
			}
			
			return rows;
		}

	// 查询函数，返回结果集
	public static ResultSet select( String sql )
	{
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection conn = null;

		if ( null != sql )
		{
			try
			{
				conn = DBConnection.getConnection();
				pstmt = conn.prepareStatement( sql );

				rs = pstmt.executeQuery( sql );
				// pstmt.close();
			}
			catch ( Exception e )
			{
				log.error( sql );
				log.error( e.getMessage(), e );
			}
			finally
			{
				if ( null != conn )
				{
					// DBConnection.close( conn );
				}
			}
		}

		return rs;
	}

	public static ResultSet select( String sql, Connection conn )
	{
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		// Connection conn = null;

		if ( null != sql )
		{
			try
			{
				// conn = DBConnection.getConnection();
				pstmt = conn.prepareStatement( sql );

				rs = pstmt.executeQuery( sql );
				// pstmt.close();
			}
			catch ( Exception e )
			{
				log.error( sql );
				log.error( e.getMessage(), e );
			}
			finally
			{
				if ( null != conn )
				{
					// DBConnection.close( conn );
				}
			}
		}

		return rs;
	}

	// 重载查询函数，返回结果集
	public static ResultSet select( Connection conn, String sql )
	{
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		if ( null != sql )
		{
			try
			{
				pstmt = conn.prepareStatement( sql );

				rs = pstmt.executeQuery( sql );
				pstmt.close();
			}
			catch ( Exception e )
			{
				log.error( sql );
				log.error( e.getMessage(), e );

			}
			finally
			{
				// 在调用者那里关闭数据库连接
				// DBConnection.close();
			}
		}

		return rs;
	}

	// 重载的查询函数，返回要查询字段的值组成的ArrayList，各ArrayList又组成ArrayList
	public static List<List<String>> select( List<String> fieldsName, String sql )
	{
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<String> singleresult = new ArrayList<String>();
		List<List<String>> result = new ArrayList<List<String>>();

		if ( fieldsName.size() > 0 && null != sql )
		{
			Connection conn = DBConnection.getConnection();
			try
			{
				pstmt = conn.prepareStatement( sql );

				rs = pstmt.executeQuery( sql );
				Iterator<String> it = fieldsName.iterator();

				while ( rs.next() )
				{
					int i = 1;
					while ( it.hasNext() )
					{
						singleresult.add( (String) rs.getObject( i ) );
						i++;
					}
					result.add( singleresult );
				}
			}
			catch ( Exception e )
			{
				log.error( sql );
				log.error( e.getMessage(), e );
			}
			finally
			{
				try
				{
					if( null !=  rs )
					{
						rs.close();
					}
					if( null != pstmt )
					{
						pstmt.close();
					}
					if ( null != conn )
					{
						DBConnection.close( conn );
					}
				}
				catch ( SQLException e )
				{
					e.printStackTrace();
				}
			}
		}
		return result;
	}

	public static List<Map<String, Object>> query( String tableName )
	{
		List<Map<String, Object>> list = new LinkedList<Map<String, Object>>();
		Connection conn = DBConnection.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "select * from " + tableName;
		try
		{
			ps = conn.prepareStatement( sql );
			rs = ps.executeQuery();
			ResultSetMetaData metaData = rs.getMetaData();
			int length = metaData.getColumnCount();
			while ( rs.next() )
			{
				Map<String, Object> map = new LinkedHashMap<String, Object>();
				for ( int i = 0; i < length; i++ )
				{
					String columName = metaData.getColumnLabel( i + 1 );
					Object obj = rs.getObject( columName );
					if ( obj != null )
					{
						map.put( columName, obj );
					}
				}
				list.add( map );
			}
		}
		catch ( SQLException e )
		{
			log.error( sql );
			log.error( e.getMessage(), e );
		}
		finally
		{
			try
			{
				if(  null != rs )
				{
					rs.close();
				}
				if( null != ps )
				{
					ps.close();
				}
				if ( null != conn )
				{
					DBConnection.close( conn );
				}
			}
			catch ( SQLException e )
			{
				e.printStackTrace();
			}
		}
		return list;
	}

	public static List<Map<String, Object>> queryBySql( String sql )
	{
		List<Map<String, Object>> list = new LinkedList<Map<String, Object>>();
		Connection conn = DBConnection.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try
		{
			ps = conn.prepareStatement( sql );
			rs = ps.executeQuery();
			ResultSetMetaData metaData = rs.getMetaData();
			int length = metaData.getColumnCount();
			while ( rs.next() )
			{
				Map<String, Object> map = new LinkedHashMap<String, Object>();
				for ( int i = 0; i < length; i++ )
				{
					String columName = metaData.getColumnLabel( i + 1 );
					Object obj = rs.getObject( columName );
//					if ( obj != null )
//					{
						map.put( columName, obj );
//					}
				}
				list.add( map );
			}
		}
		catch ( SQLException e )
		{
			log.error( sql );
			log.error( e.getMessage(), e );
		}
		finally
		{
			try
			{
				if(  null != rs )
				{
					rs.close();
				}
				if( null != ps )
				{
					ps.close();
				}
				if ( null != conn )
				{
					DBConnection.close( conn );
				}
			}
			catch ( SQLException e )
			{
				e.printStackTrace();
			}
		}
		
		return list;
	}
	
	public static List<Map<String, Object>> queryBySql( Connection conn, String sql )
	{
		List<Map<String, Object>> list = new LinkedList<Map<String, Object>>();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try
		{
			ps = conn.prepareStatement( sql );
			rs = ps.executeQuery();
			ResultSetMetaData metaData = rs.getMetaData();
			int length = metaData.getColumnCount();
			while ( rs.next() )
			{
				Map<String, Object> map = new LinkedHashMap<String, Object>();
				for ( int i = 0; i < length; i++ )
				{
					String columName = metaData.getColumnLabel( i + 1 );
					Object obj = rs.getObject( columName );
					if ( obj != null )
					{
						map.put( columName, obj );
					}
				}
				list.add( map );
			}
		}
		catch ( SQLException e )
		{
			log.error( sql );
			log.error( e.getMessage(), e );
		}
		finally
		{
			try
			{
				if(  null != rs )
				{
					rs.close();
				}
				if( null != ps )
				{
					ps.close();
				}
			}
			catch ( SQLException e )
			{
				log.error( e.getMessage(), e );
			}
		}
		return list;
	}

	public static void close( ResultSet rs, PreparedStatement pstmt, Connection conn )
	{
		try
		{
			if ( rs != null )
			{
				rs.close();
			}
			if ( pstmt != null )
			{
				pstmt.close();
			}
			if ( conn != null )
			{
				conn.close();
			}
		}
		catch ( SQLException e )
		{
			log.error( e.getMessage(), e );		
		}
	}

	public static Date getMaxTime()
	{
		Date time = null;
		Connection conn = DBConnection.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "select max(Date) from F_Positions";
		try
		{
			ps = conn.prepareStatement( sql );
			rs = ps.executeQuery();
			ResultSetMetaData metaData = rs.getMetaData();
			int length = metaData.getColumnCount();
			while ( rs.next() )
			{
				for ( int i = 0; i < length; i++ )
				{
					String columName = metaData.getColumnLabel( i + 1 );
					time = (Date) rs.getObject( columName );
				}
			}
		}
		catch ( SQLException e )
		{
			log.error( e.getMessage(), e );	
		}
		finally
		{
			if( null != ps )
			{
				try
				{
					ps.close();
				}
				catch ( SQLException e )
				{
					e.printStackTrace();
				}
			}
			if ( null != conn )
			{
				DBConnection.close( conn );
			}
		}
		return time;
	}
	
	//获得表的字段名称，返回list
	public static List<String> getDBColumns( String tableName )
	{
		List<String> columnList = new ArrayList<String>();
		Connection conn = DBConnection.getConnection();

		String sql = "SELECT * FROM " + tableName + " limit 1 ";
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			pstmt = conn.prepareStatement( sql );
			rs = pstmt.executeQuery();
			// 结果集元数据
			ResultSetMetaData rsMetaData = rs.getMetaData();
			// 字段数量
			int colCount = rsMetaData.getColumnCount();

			// 读取字段名到数组
			for ( int i = 1; i <= colCount; i++ )
			{
				columnList.add( rsMetaData.getColumnName( i ) );
			}

		}
		catch ( SQLException e )
		{
			e.printStackTrace();
		}

		close( rs, pstmt, conn );
		return columnList;
	}
}
