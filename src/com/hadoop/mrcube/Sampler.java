package com.hadoop.mrcube;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Sampler {
	
	private Path input;
	private Path output;
	private Random random = new Random();
	
	public Sampler(String input, String output){
		this.input = new Path(input);
		this.output = new Path(output);
	}
	
	public void sampling(Configuration conf){
		try{
			FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream out = fs.create(output);
			
			BufferedReader br = new BufferedReader(new FileReader(input.toString()));
		    
		    double r = MRCubeSetting.limitTuplesPerReducer / (double) MRCubeSetting.dataSize;
		    int nNeededRecords = (int)(100 / r) + 1;
		    int nCreatedRecords = 0;
		    
		    String line;
		    while ((line = br.readLine()) != null) {
		    	if (random.nextInt(1000) == 44){
		    		out.writeChars(line + "\n");
		    		nCreatedRecords++;
		    	}
		    	if (nCreatedRecords == nNeededRecords)
		    		break;
		    }
		    
		    br.close();
		    out.close();
		}catch(Exception ex){
			System.out.println(ex.getMessage());
		}
	}
}
