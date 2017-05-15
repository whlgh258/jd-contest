package com.jd.may.fifteen;

import java.text.DecimalFormat;

public class DigitalFormat
{
	public static String format( int num )
	{
		DecimalFormat format = new DecimalFormat( "00" );
		String s = format.format( num );
		
		return s;
	}
	
	public static String format( double num )
	{
		DecimalFormat format = new DecimalFormat( "0.00" );
		String s = format.format( num );
		
		return s;
	}
	
	public static double formatForDouble( double num )
	{
		if(Math.abs(num) < 1e-10){
			return 0.0;
		}

		try
		{
			DecimalFormat format = new DecimalFormat( "0.000" );
			String s = format.format( num );
			
			return Double.parseDouble( s );
		}
		catch ( NumberFormatException e )
		{
			return 0.0;
		}
	}

	public static double formatForPercentDouble( double num )
	{
		try
		{
			DecimalFormat format = new DecimalFormat( "0.0000" );
			String s = format.format( num );

			return Double.parseDouble( s );
		}
		catch ( NumberFormatException e )
		{
			return 0.0;
		}
	}

	public static String formatForPercent( double num )
	{
		try
		{
			DecimalFormat format = new DecimalFormat("0.00%");
			String s = format.format( num );

			return s;
		}
		catch ( NumberFormatException e )
		{
			return "0.0%";
		}
	}

	public static String groupNumber(long num){
		DecimalFormat df=(DecimalFormat) DecimalFormat.getInstance();
		df.setGroupingSize(3);

		return df.format(num);
	}
	
	public static void main( String[] args )
	{
		System.out.println( format( -1) );
		System.out.println(formatForPercent(0.05));
	}
}
