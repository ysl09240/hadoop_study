package com.slin.hadoop;

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
 * @author :  SongLin.Yang [ysl09240@gmail.com]
 * @version :  1.0
 * @encoding : UTF-8
 * @package : com.slin.hadoop
 * @link :  http://lezhai365.com
 * @created on   :  15-4-8
 * @copyright :  Copyright(c) 2013 西安乐宅网络科技有限公司
 * @description :
 */
public class NewMaxTemperature {
    static class NewMaxTemperatureMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        private static final int MISSING = 9999;

        public void map(LongWritable key ,Text value,Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String year = line.substring(15,19);
            int airTemperature = 0;
            if(line.charAt(87) == '+'){
                airTemperature = Integer.parseInt(line.substring(88,92));
            }else {
                airTemperature = Integer.parseInt(line.substring(87,92));
            }

            String quality = line.substring(92,93);
            if(airTemperature != MISSING &&quality.matches("[01459]")){
                context.write(new Text(year),new IntWritable(airTemperature));
            }
        }


    }
    static class NewMaxTemperatureReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int maxValue = Integer.MIN_VALUE;
            for(IntWritable value : values){
                maxValue = Math.max(maxValue,value.get());
            }
            context.write(key,new IntWritable(maxValue));
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length != 2){
            System.err.println("Usage:NewMaxTemperature <input path>");
            System.exit(-1);
        }
        //new job
        Job job = new Job();
        //设置jarclass 文件
        job.setJarByClass(NewMaxTemperature.class);
        //设置用输入流读入的文件
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //设置用输入流要写入的文件
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //指定map 类
        job.setMapperClass(NewMaxTemperatureMapper.class);
        //指定 reducer类
        job.setReducerClass(NewMaxTemperatureReducer.class);
        //指定map和reducer的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
