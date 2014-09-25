package com.hadoop.mrcube;

import com.hadoop.buc.InputReader;

public class AirPlaneReader implements InputReader{
	String[] sources;
	
	@Override
	public void initWithString(String input) {
		sources = input.split(",");
		
	}

	@Override
	public String getValueByAttributeName(String name) {
		
		if (name.equals("Year"))
			return sources[0];
		if (name.equals("Month"))
			return sources[1];
		if (name.equals("DayofMonth"))
			return sources[2];
		if (name.equals("DayOfWeek"))
			return sources[3];
		
		if (name.equals("FlightNum"))
			return sources[9];
		
		if (name.equals("AirTime"))
			return sources[13];
		if (name.equals("ArrDelay"))
			return sources[14];
		if (name.equals("DepDelay"))
			return sources[15];
		
		if (name.equals("Dest"))
			return sources[17];
		if (name.equals("Distance"))
			return sources[18];
		
		if (name.equals("CancellationCode"))
			return sources[22];
		if (name.equals("Diverted"))
			return sources[23];
		
		if (name.equals("Count"))
			return "1";
		
		return null;
	}

}
