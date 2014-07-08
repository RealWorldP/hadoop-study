package com.myhadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Created by nine on 14-7-8.
 */
public class MaxTemperatureMapperTest {

    @Test
    public void processValidRecord() throws IOException {
        MaxTemperatureMapper mapper = new MaxTemperatureMapper();
        Text text = new Text("0043011990999991950051518004+68750+023550FM-12+0382"+
                                "99999V0203201N00261220001CN9999999N9-00111+9999999999");
        OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);
        mapper.map(null, text, output, null);
        verify(output).collect(new Text("1950"), new IntWritable(-11));

    }

    @Test
    public void ignoresMissingTemperatureRecord() throws IOException {
        MaxTemperatureMapper mapper = new MaxTemperatureMapper();
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                "99999V0203201N00261220001CN9999999N9+99991+99999999999");
        OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);
        mapper.map(null, value, output, null);
        verify(output, never()).collect(any(Text.class), any(IntWritable.class));
    }
}
