package org.murphy.summarizer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StatsTuple implements Writable {
  private double min = Double.NaN;
  private double max = Double.NaN;
  private long count = 0;
  
  public double getMin() {
    return min;
  }
  public void setMin(double min) {
    this.min = min;
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
  
  @Override
  public void readFields(DataInput in) throws IOException {
    // Read the data out in the order it is written,
    //  creating new Date objects from the UNIX timestamp
    min = in.readDouble();
    max = in.readDouble();
    count = in.readLong();
  }
  @Override
  public void write(DataOutput out) throws IOException{
    //Write the data out in the order it is read,
    //using the UNIX timestamp to represent the Date
    out.writeDouble(min);
    out.writeDouble(max);
    out.writeLong(count);
  }
  public String toString() {
    // return frmt.format(min) + "\t" + frmt.format(max) + "\t" + count;
    return String.valueOf(min) +"\t" + String.valueOf(max) + count;
  } 
  
  
}