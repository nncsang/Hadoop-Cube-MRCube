package com.hadoop.mrcube;

public class MRCubeSetting {
	public static int limitTuplesPerReducer = 2;
	public static int dataSize = 9;
	public static int r = limitTuplesPerReducer / dataSize;
	public static int N = 100 / r + 1;
}
