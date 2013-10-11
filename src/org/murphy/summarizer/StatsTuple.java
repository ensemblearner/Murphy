package org.murphy.summarizer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StatsTuple implements Writable {
  private double min = Double.MAX_VALUE;
  private double max = Double.MIN_VALUE;
  private double sum = 0;
  private long count = 0;
  private double mean = 0;
  public StatsTuple(){}
  
  
  public double getMin() {
    return min;
  }
  public void setMin(double min) {
    this.min = min;
  }
  public void setSum(double sum){
	  this.sum = sum;
  }
  public double getSum(){
	  return this.sum;
  }
  public double getMax() {
    return max;
  }
  public void setMax(double max) {
    this.max = max;
  }
  public long getCount() {
    return count;
  } 
  public void setCount(long count) {
    this.count = count;
  }
  
  public double getMean() {
	  if (this.count == 0)
		   return 0;
	  return this.sum/ this.count;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    min = in.readDouble();
    max = in.readDouble();
    count = in.readLong();
  }
  @Override
  public void write(DataOutput out) throws IOException{
    out.writeDouble(min);
    out.writeDouble(max);
    out.writeLong(count);
  }
  public String toString() {
    // return frmt.format(min) + "\t" + frmt.format(max) + "\t" + count;
    return "min= " + String.valueOf(min) +"\t max= " + String.valueOf(max) + "\t mean= "+ String.valueOf(getMean()); 
  } 
  
  
}