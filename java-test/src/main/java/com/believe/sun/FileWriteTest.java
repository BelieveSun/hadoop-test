package com.believe.sun;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

/**
 * Created by sun.gj on 2017/3/20.
 */
public class FileWriteTest {
    public static void main(String[]args){
        String uri = "hdfs://10.19.31.139:9000/user/hadoop/testOutput/abc.txt";
        Configuration conf = new Configuration();
        FSDataOutputStream out = null;
        FSDataInputStream in = null;
        FileSystem fs = null;
        try {
             fs = FileSystem.get(URI.create(uri), conf);
            out = fs.create(new Path(uri), () -> System.out.println("*"));
            out.writeUTF("a b c hello");
        } catch (IOException e) {
            e.printStackTrace();
        }
        IOUtils.closeStream(out);
        try {
            if (fs != null) {
                in = fs.open(new Path(uri));
                IOUtils.copyBytes(in,System.out,4096,true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        IOUtils.closeStream(in);
    }
}
