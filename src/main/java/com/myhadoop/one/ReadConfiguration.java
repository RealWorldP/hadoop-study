package com.myhadoop.one;

import org.apache.hadoop.conf.Configuration;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * Created by nine on 14-7-8.
 */
public class ReadConfiguration {

    public static void main(String[] args){
        Configuration conf = new Configuration();
        conf.addResource("configuration-1.xml");
        conf.addResource("configuration-2.xml");
        assertThat(conf.get("color"), is("yellow"));
        assertThat(conf.getInt("size", 0), is(12));
        assertThat(conf.get("weight"), is("heavy"));
        assertThat(conf.get("breadth", "wide"), is("wide"));
        assertThat(conf.get("size-weight"), is("12,heavy"));
        System.setProperty("size", "14");
        System.out.println(conf.get("size-weight"));
    }
}
