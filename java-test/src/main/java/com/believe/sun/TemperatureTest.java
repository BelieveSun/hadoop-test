package com.believe.sun;

import com.believe.sun.util.HDFSFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by sun.gj on 2017/3/20.
 */
public class TemperatureTest {

    public static class MinTemperatureMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String year = line.substring(15,19);
            int air ;
            if(line.charAt(87) == '+'){
                air =Integer.parseInt(line.substring(88,92));
            }else {
                air = Integer.parseInt(line.substring(87,92));
            }
            String quality = line.substring(92,93);
            if(air != 9999 && quality.matches("[01459]")){
                context.write(new Text(year),new IntWritable(air));
            }
        }
    }

    public static class MinTemperatureReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int minValue = Integer.MAX_VALUE;
            for(IntWritable value : values){
               minValue = Math.min(minValue,value.get());
            }
            context.write(key,new IntWritable(minValue));
        }
    }

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        String inputFile = "hdfs://10.19.31.139:9000/user/hadoop/input/temperature";
        String output = "hdfs://10.19.31.139:9000/user/hadoop/temperatureOutput";
        Configuration conf = new Configuration();
        conf.set("mapred.jar","D:\\Work\\hadoop-test\\java-test\\target\\java-test-1.0-SNAPSHOT.jar");
        HDFSFileUtil.deleteDir(conf,output);
        Job job = Job.getInstance(conf, "min temperature");
        job.setJarByClass(TemperatureTest.class);
        job.setMapperClass(MinTemperatureMapper.class);
        job.setCombinerClass(MinTemperatureReduce.class);
        job.setReducerClass(MinTemperatureReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
