package com.myhadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by nine on 14-7-9.
 */
public class MaxTemperatureReducer extends MapReduceBase
                    implements Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    public void reduce(Text key, Iterator<IntWritable> values,
                       OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int maxValue = Integer.MIN_VALUE;
        while (values.hasNext()){
            maxValue = Math.max(maxValue, values.next().get());
        }
        outputCollector.collect(key, new IntWritable(maxValue));
    }
}
