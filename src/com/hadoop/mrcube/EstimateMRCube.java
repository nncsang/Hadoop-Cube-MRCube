package com.hadoop.mrcube;

import com.hadoop.mrcube.EstimateMRCubeMapper.GroupComparator;
import com.hadoop.mrcube.TextPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.buc.BUC;
import com.hadoop.buc.InputReader;
import com.hadoop.buc.InventoryReader;

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
		Configuration conf = this.getConf();
		conf.set("attributeNames", "Item\tColor\tStore");
		conf.set("measuredAttributeName", "Quantity");
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		    
		Job job = new Job(conf, "EstimateMRCube");
		
		job.setInputFormatClass(TextInputFormat.class);
		    
		job.setMapperClass(EstimateMRCubeMapper.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		    
		job.setReducerClass(EstimateMRCubeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		    
		//job.setCombinerClass(EstimateMRCubeReducer.class);
		job.setPartitionerClass(EstimateMRCubePartitioner.class);
		//job.setGroupingComparatorClass(GroupComparator.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		    
		FileInputFormat.addInputPath(job, inputFile);
		    
		FileOutputFormat.setOutputPath(job, outputDir);
		    
		job.setNumReduceTasks(numReducers);
		    
		job.setJarByClass(NaiveMRCube.class);
		
		return job.waitForCompletion(true) ? 0 : 1; 
	}
}

class EstimateMRCubeMapper extends Mapper<LongWritable, Text, TextPair, IntWritable>{
	private InputReader reader = new InventoryReader();
	private String[] attributeNames;
	
	private BUC buc;
	private Set<String> cubeRegions;
	private List<List<String>> cubeRegionsList = new ArrayList<List<String>>();
	
	private IntWritable one = new IntWritable(1);
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
	    	
	    	TextPair keyTxt = new TextPair(new Text(BUC.join(region, ",")), new Text(BUC.join(key, ",")));	    	
	    	context.write(keyTxt, one);
	    }
	}
	
	public static class GroupComparator extends WritableComparator { 
		protected GroupComparator() {
			super(TextPair.class, true); 
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			String first = ((TextPair) a).getFirst().toString();
			String second = ((TextPair) a).getFirst().toString();
			
			return first.compareTo(second);
		}
	}
}

class EstimateMRCubeReducer extends Reducer<TextPair, IntWritable, Text, IntWritable>{
	private InputReader reader = new InventoryReader();
	private Map<String, Integer> regions = new HashMap<String, Integer>();
	
	@Override
	protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		List<Integer> numOfTuples = new ArrayList<Integer>();
		Text region = key.getFirst();
		numOfTuples.add(0);
		int max = 0;
		for(IntWritable value: values){
			max++; 
		}
		
		if (regions.containsKey(region.toString()) == false){
			regions.put(region.toString(), max);
		}else{
			int curMax = regions.get(region.toString());
			if (curMax < max){
				regions.put(region.toString(), max);
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Set<Entry<String, Integer>> entries = regions.entrySet();
		for(Entry<String, Integer> entry: entries){
			context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
		}
	}
}

class EstimateMRCubePartitioner extends Partitioner<TextPair, IntWritable> {
	@Override
	public int getPartition(TextPair key, IntWritable value, int numPartitions) {	
		int partition =  (key.getFirst().hashCode()) % numPartitions;
		if (partition < 0)
			partition = - partition;
		return partition;
	}
}
