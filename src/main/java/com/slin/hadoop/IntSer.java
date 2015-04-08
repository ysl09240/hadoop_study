package com.slin.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

import java.io.*;

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
public class IntSer {

    public static byte[] serialize(Writable w) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataout = new DataOutputStream(out);
        w.write(dataout);
        dataout.close();
        return out.toByteArray();
    }

    public static Writable deserialize(Writable w, byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream datain =  new DataInputStream(in);
        w.readFields(datain);
        return w;

    }

    public static void main(String[] args) throws IOException {
        IntWritable intw = new IntWritable(7);
        byte[] bytes= serialize(intw);
        System.out.println(intw);

        IntWritable intw2 = new IntWritable(0);
        intw2 = (IntWritable) deserialize(intw2,bytes);
        System.out.println(intw2);

    }


}
