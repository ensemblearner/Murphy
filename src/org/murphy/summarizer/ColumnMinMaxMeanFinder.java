package org.murphy.summarizer;
/*
 * Untested code..
 * ToDo.. Run on hadoop cluster
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

public class ColumnMinMaxMeanFinder extends Configured implements Tool {
	
	

	/**
	 * @param args
	 */
	
	public  static class ColumnMinMaxMeanrMapper extends Mapper<LongWritable, Text, Text, StatsTuple>{
		
		private ArrayList<Double> list = new ArrayList<Double>();
		private StatsTuple tuple = new StatsTuple();
		private String delimiter;
		private int index = -1;
		private Text stats = new Text("stats");
		
		@Override
		  protected void setup(Context context) throws IOException, InterruptedException {
		    Configuration conf = context.getConfiguration();
		    delimiter = conf.get("delimiter");
		    index = conf.getInt("index",-1);
		    if (index == -1)
		    	System.exit(-1);
		  }
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException{
			
			double sum = 0;
			int count = 0;
			Iterator<Double> itr = list.iterator();
			while(itr.hasNext()){
				double curNum = itr.next();
				sum += curNum;
				count++;
				if (curNum < tuple.getMin())
					tuple.setMin(curNum);
				if (curNum > tuple.getMax())
					tuple.setMax(curNum);	
			}
			tuple.setSum(sum);
			tuple.setCount(count);
			context.write(stats, tuple);
		}
		
		 public void map(LongWritable key, Text value , Context context) throws IOException, InterruptedException{
			 String [] stringChunks = value.toString().split(delimiter);
			 list.add(Double.parseDouble(stringChunks[index]));
		 
		 }
		
		
		
	}
	
	
	public static class ColumnMinMaxMeanrReducer extends Reducer<Text, StatsTuple, NullWritable, Text> {
		private double absoluteMax = Double.MAX_VALUE;
		private double absoluteMin = Double.MIN_VALUE;
		
		  // Our output value Writable 
		  private StatsTuple result = new StatsTuple();
		  public void reduce(Text key, Iterable<StatsTuple> values,Context context) throws IOException, InterruptedException {
		    // Initialize our result
		    result.setMin(absoluteMin);
		    result.setMax(absoluteMax);
		    result.setCount(0);
		    double sum = 0;
		    int count = 0;
		    //  Iterate through all input values for this key
		    for (StatsTuple val : values) {
		    	if (val.getMin() < result.getMin())
		    		result.setMin(val.getMin());
		    	if (val.getMax() > result.getMax())
		    		result.setMax(val.getMax());
		    	sum += val.getSum();
		    	count += val.getCount();
		   
		  }
		    result.setCount(count);
		    result.setSum(sum);
		    String output =result.toString();
		    context.write(NullWritable.get(), new Text(output));

		    
		}
	}
	
	
	 public static void main(String[] args) throws Exception {
		    int res = ToolRunner.run(new Configuration(), new ColumnMinMaxMeanFinder(), args);
		    System.exit(res);
		  }
		  
		  @Override
		  public int run(String[] args) throws Exception {
		    
		    // When implementing tool
		    Configuration conf = this.getConf();
		    
		    // Create job
		    Job job = new Job(conf, "Tool Job");
		    job.setJarByClass(ColumnMinMaxMeanFinder.class);
		    
		    // Setup MapReduce job
		    // Do not specify the number of Reducer
		    job.setMapperClass(ColumnMinMaxMeanrMapper.class);
		    //job.setMapperClass(Mapper.class);
		    job.setReducerClass(ColumnMinMaxMeanrReducer.class);
		    
		    // Specify key / value
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(StatsTuple.class);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    
		    // Input
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    job.setInputFormatClass(TextInputFormat.class);
		    
		    // Output
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    job.setOutputFormatClass(TextOutputFormat.class);
		    
		    
		    // Set schema info
		    conf.set("delimiter", args[2]);
		    conf.set("index",args[3]);
		    
		    // Execute job and return status
		    return job.waitForCompletion(true) ? 0 : 1;
		  }

}
