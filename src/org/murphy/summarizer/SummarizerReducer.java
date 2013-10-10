package org.murphy.summarizer;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SummarizerReducer extends Reducer<Text, StatsTuple, Text, StatsTuple> {
  // Our output value Writable 
  private StatsTuple result = new StatsTuple();
  public void reduce(Text key, Iterable<StatsTuple> values,Context context) throws IOException, InterruptedException {
    // Initialize our result
    result.setMin(Double.NaN);
    result.setMax(Double.NaN);
    result.setCount(0);
    int sum = 0;
    //  Iterate through all input values for this key
    for (StatsTuple val : values) {
      //  If the value's min is less than the result's min
      //  Set the result's min to value's
      if (result.getMin() == Double.NaN || val.getMin() < result.getMin()) {
        result.setMin(val.getMin());
      }
      // If the value's max is more than the result's max
      // Set the result's max to value's
      if (result.getMax() == Double.NaN || val.getMax() > result.getMax()) {
        result.setMax(val.getMax());
      }
      // Add to our sum the count for value
      sum += val.getCount();
    }
    //  Set our count to the number of input values
    result.setCount(sum);
    context.write(key, result);
  }
}