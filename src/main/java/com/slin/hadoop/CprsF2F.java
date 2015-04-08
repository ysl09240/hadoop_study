package com.slin.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

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
public class CprsF2F {
    public static void main(String[] args) throws ClassNotFoundException, IOException {
        if(args.length != 3){
            System.err.println("Usage:CprsF2F cmps_name src target");
            System.exit(2);
        }

        Class<?> codecClass = Class.forName(args[0]);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,conf);

        InputStream in = null;
        OutputStream out = null;
        FileSystem fs = FileSystem.get(URI.create(args[0]),conf);
        try {
            in = fs.open(new Path(args[1]));
            out = codec.createOutputStream(fs.create(new Path(args[2])));
            IOUtils.copyBytes(in, out, conf);
        }finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }





    }
}
