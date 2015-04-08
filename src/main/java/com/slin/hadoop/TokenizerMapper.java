package com.slin.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author :  SongLin.Yang [ysl09240@gmail.com]
 * @version :  1.0
 * @encoding : UTF-8
 * @package : com.slin.hadoop
 * @link :  http://lezhai365.com
 * @created on   :  15-4-1
 * @copyright :  Copyright(c) 2013 西安乐宅网络科技有限公司
 * @description :
 */
public class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable> {
    IntWritable one =  new IntWritable(1);
    Text word = new Text();
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
       StringTokenizer itr = new StringTokenizer(value.toString());
       while(itr.hasMoreTokens()){
           word.set(itr.nextToken());
           context.write(word,one);
       }
    }


}
