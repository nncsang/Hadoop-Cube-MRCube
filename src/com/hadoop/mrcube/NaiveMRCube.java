package com.hadoop.mrcube;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.buc.BUC;
import com.hadoop.buc.InputReader;
import com.hadoop.buc.InventoryReader;

public class NaiveMRCube extends Configured implements Tool {
	private int numReducers;
	private Path inputFile;
	private Path outputDir;
	
	public static void main(String args[]) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new NaiveMRCube(args), args);
	    System.exit(res);
	}
	
	public NaiveMRCube(String[] args) {
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
		Configuration conf = this.getConf();
		conf.set("attributeNames", "Item\tColor\tStore");
		conf.set("measuredAttributeName", "Quantity");
		
		//if file output is existed, delete it
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		    
		Job job = new Job(conf, "NaiveMRCube"); // TODO: define new job instead of null using conf
		
		// TODO: set job input format
		job.setInputFormatClass(TextInputFormat.class);
		    
		// TODO: set map class and the map output key and value classes
		job.setMapperClass(NaiveMRCubeMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		    
		// TODO: set reduce class and the reduce output key and value classes
		job.setReducerClass(NaiveMRCubeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		    
		// TODO: set job output format
		job.setOutputFormatClass(TextOutputFormat.class);
		    
		// TODO: add the input file as job input (from HDFS) to the variable
		//       inputFile
		FileInputFormat.addInputPath(job, inputFile);
		    
		// TODO: set the output path for the job results (to HDFS) to the variable
		//       outputPath
		FileOutputFormat.setOutputPath(job, outputDir);
		    
		// TODO: set the number of reducers using variable numberReducers
		job.setNumReduceTasks(numReducers);
		    
		    // TODO: set the jar class
		job.setJarByClass(NaiveMRCube.class);
		
		return job.waitForCompletion(true) ? 0 : 1; // this will execute the job		
	}
}

class NaiveMRCubeMapper extends Mapper<LongWritable, Text, Text, Text>{
	private InputReader reader = new InventoryReader();
	private String[] attributeNames;
	
	private BUC buc;
	private Set<String> cubeRegions;
	private List<List<String>> cubeRegionsList = new ArrayList<List<String>>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		attributeNames = conf.get("attributeNames").split("\t");
		
		buc = new BUC(null, attributeNames, null, attributeNames.length, 0, null);
		cubeRegions = buc.cubeRegions();
		
		Iterator<String> itor = cubeRegions.iterator();
		while(itor.hasNext()){
			String[] attributes = itor.next().split(",");
			cubeRegionsList.add(Arrays.asList(attributes));
		}
	}
	
	@Override
	protected void map(LongWritable index, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		reader.initWithString(line);
		
	    Iterator<List<String>> itor = cubeRegionsList.iterator();
	    while(itor.hasNext()){
	    	List<String> region = itor.next();
	    	
	    	List<String> key = new ArrayList<String>();
	    	Iterator<String> itor1 = region.iterator();
	    	while(itor1.hasNext()){
	    		String attribute = itor1.next();
	    		if (attribute.equals("*")){
	    			key.add("*");
	    		}else{
	    			key.add(reader.getValueByAttributeName(attribute));
	    		}
	    	}
	    	
	    	Text keyTxt = new Text(BUC.join(key, ","));
	    	Text valueTxt = new Text(line);
	    	
	    	context.write(keyTxt, valueTxt);
	    }
	}
}

class NaiveMRCubeReducer extends Reducer<Text, Text, Text, IntWritable>{
	private InputReader reader = new InventoryReader();
	private String measuredAttributeName;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		measuredAttributeName = context.getConfiguration().get("measuredAttributeName");
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for(Text value: values){
			reader.initWithString(value.toString());
			int num = Integer.parseInt(reader.getValueByAttributeName(measuredAttributeName));
			sum += num;
		}
		
		context.write(new Text("(" + key.toString() + ")"), new IntWritable(sum));
	}
}
