package org.murphy.summarizer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public  class SummarizerMapper extends Mapper<LongWritable, Text, Text, StatsTuple>{
  private Text outId = new Text();
  private StatsTuple tuple= new StatsTuple();
  private int groupByIndex = -1;
  private int groupIndex = -1;
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    String schema = conf.get("schema");
    String[] schemaTokens = schema.split(",");
    List<String> schemaList = Arrays.asList(schemaTokens);
    String groupBy = conf.get("groupBy");
    String group = conf.get("group");
    groupByIndex = schemaList.indexOf(groupBy);
    groupIndex = schemaList.indexOf(group);
  }
  
  @Override
  public void map(LongWritable key, Text value , Context context) throws IOException, InterruptedException{
    String[] inChunks = value.toString().split(",");
    String groupByEle = inChunks[groupByIndex]; 
    double groupEle = Double.parseDouble(inChunks[groupIndex]);
    
    tuple.setMin(groupEle);
    tuple.setMax(groupEle);
    tuple.setCount(1);
    outId.set(groupByEle);
    context.write(outId,tuple);
  }
}




