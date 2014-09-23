package com.hadoop.mrcube;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EstimateMRCube extends Configured implements Tool {
	private int numReducers;
	private Path inputFile;
	private Path outputDir;
	
	public static void main(String args[]) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new EstimateMRCube(args), args);
	    System.exit(res);
	}
	
	public EstimateMRCube(String[] args) {
	    if (args.length != 3) {
	      System.out.print(args.length);
	      System.out.println("Usage: WordCount <num_reducers> <input_path> <output_path>");
	      System.exit(0);
	    }
	    
	    this.numReducers = Integer.parseInt(args[0]);
	    this.inputFile = new Path(args[1]);
	    this.outputDir = new Path(args[2]);
	  }
	
	@Override
	public int run(String[] arg0) throws Exception {
		return 0;
	}

}
