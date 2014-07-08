package com.myhadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by nine on 14-7-8.
 */
public class MaxTemperatureMapper extends MapReduceBase
                implements Mapper<LongWritable, Text, Text, IntWritable>{
    @Override
    public void map(LongWritable key, Text value,
                    OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {

        String line = value.toString();
        String year = line.substring(15,19);
        String temp = line.substring(87,92);
        if(!missing(temp)){
            int airTemperature = Integer.parseInt(line.substring(87, 92));
            outputCollector.collect(new Text(year), new IntWritable(airTemperature));
        }
    }

    private boolean missing(String temp){
       return "+9999".equals(temp);
    }
}
