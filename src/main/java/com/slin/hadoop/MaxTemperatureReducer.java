package com.slin.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

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
public class MaxTemperatureReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    public void reduce(Text key, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int maxValue = Integer.MIN_VALUE;
        while(iterator.hasNext()){
            maxValue = Math.max(maxValue,iterator.next().get());
        }
        outputCollector.collect(key,new IntWritable(maxValue));
    }
}
