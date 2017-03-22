package com.believe.sun;

import com.believe.sun.util.HDFSFileUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by sun.gj on 2017/3/22.
 */
public class DeptTest extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(DeptTest.class);
    private static final String HDFS_INPUT = "hdfs://10.19.31.139:9000/user/hadoop/input";
    private static final String HDFS_OUTPUT = "hdfs://10.19.31.139:9000/user/hadoop/output/dept";
    private static final String URL = "D:\\hadoop\\hadoop-test-data";


    public static void addFileToHdfs(String uri,FileSystem fs,Configuration conf){
        String baseName = FilenameUtils.getBaseName(uri);
        String extension = FilenameUtils.getExtension(uri);
        String path = "input/"+baseName+"/"+baseName+"."+extension;
        try {
            HDFSFileUtil.deleteDir(conf,HDFS_INPUT+"/"+baseName);
            FileUtil.copy(new File(uri), fs,new Path(path),true,conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class DeptMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Map<String,String> deptMap = new HashMap<>();
        private String[] kv;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            BufferedReader in = null;
            String deptIdName;
            for (URI uri : cacheFiles) {
                LOG.info("URI:"+uri);
                // 对部门文件字段进行拆分并缓存到deptMap中
                if (uri.toString().contains("dept")) {
                    FileSystem fileSystem = FileSystem.get(uri, context.getConfiguration());
                    FSDataInputStream open = fileSystem.open(new Path(uri));
                    in = new BufferedReader(new InputStreamReader(open));
                    while (null != (deptIdName = in.readLine())) {

                        // 对部门文件字段进行拆分并缓存到deptMap中
                        // 其中Map中key为部门编号，value为所在部门名称
                        deptMap.put(deptIdName.split(",")[0], deptIdName.split(",")[1]);
                    }
                }
            }

            in.close();
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 对员工文件字段进行拆分
            kv = value.toString().split(",");

            // map join: 在map阶段过滤掉不需要的数据，输出key为部门名称和value为员工工资
            if (deptMap.containsKey(kv[7])) {
                if (null != kv[5] && !"".equals(kv[5].toString())) {
                    context.write(new Text(deptMap.get(kv[7].trim())), new Text(kv[5].trim()));
                }
            }
        }
    }

    public static class DeptReduce extends Reducer<Text,Text,Text,LongWritable>{
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // 对同一部门的员工工资进行求和
            long sumSalary = 0;
            for (Text val : values) {
                sumSalary += Long.parseLong(val.toString());
            }

            // 输出key为部门名称和value为该部门员工工资总和
            context.write(key, new LongWritable(sumSalary));
        }
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapred.jar","D:\\Work\\hadoop-test\\java-test\\target\\java-test-1.0-SNAPSHOT.jar");

        FileSystem fs = FileSystem.get(URI.create(HDFS_INPUT), conf);

        String [] e = new String[]{"txt"};
        Collection<File> files = FileUtils.listFiles(new File(URL), e, true);
        files.forEach(file ->addFileToHdfs(file.getPath(),fs,conf));

        Job job = Job.getInstance(conf,"SumDeptSalary1");
        job.setJarByClass(DeptTest.class);
        job.setMapperClass(DeptMapper.class);
        job.setReducerClass(DeptReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(URI.create(HDFS_INPUT+"/dept/dept.txt"));
        FileInputFormat.addInputPath(job,new Path(HDFS_INPUT+"/emp"));
        HDFSFileUtil.deleteDir(conf,HDFS_OUTPUT);
        FileOutputFormat.setOutputPath(job,new Path(HDFS_OUTPUT));

        job.waitForCompletion(true);

        return job.isSuccessful()?0:1;
    }

    public static void main(String [] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new DeptTest(), args);
        System.exit(run);
    }
}
