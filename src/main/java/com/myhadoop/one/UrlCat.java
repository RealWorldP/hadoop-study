package com.myhadoop.one;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by nine on 6/26/14.
 */
public class UrlCat {
//    static {
//        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
//    }

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(uri));
            System.out.println("First");
            IOUtils.copyBytes(in, System.out, 4096, false);
            in.seek(0);
            System.out.println("Second");
            IOUtils.copyBytes(in, System.out, 4096, false);

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(in);
        }
    }
}
