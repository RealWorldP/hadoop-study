package com.myhadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by nine on 14-7-9.
 */
public class MaxTemperatureReducerText {

    @Test
    public void returnMaxIntegerValues() throws IOException {
        MaxTemperatureReducer reducer = new MaxTemperatureReducer();
        Text key = new Text("1950");
        Iterator<IntWritable> values = Arrays.asList(new IntWritable(5), new IntWritable(10)).iterator();
        OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);
        reducer.reduce(key, values, output, null);
        verify(output).collect(key, new IntWritable(5));
    }
}
