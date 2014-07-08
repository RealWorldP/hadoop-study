package com.myhadoop.one;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Created by nine on 14-7-1.
 */
public class CheckFilePOSIX {

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path("p");
        FSDataOutputStream outputStream = fs.create(path);
        outputStream.write("content".getBytes());
//        outputStream.hflush();
        outputStream.close();
        System.out.println(fs.getFileStatus(path).getLen());
        assertThat(fs.getFileStatus(path).getLen(), is(0L));
    }
}
