package com.slin.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
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
public class FileCopy {
    public static void main(String[] args) throws IOException {
        if(args.length != 2){
            System.out.println("Usage:filecopy<source><target>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        InputStream in = new BufferedInputStream(new FileInputStream(args[0]));
        FileSystem fs = FileSystem.get(URI.create(args[1]),conf);
        OutputStream out = fs.create(new Path(args[1]));
        IOUtils.copyBytes(in, out, 4096, true);
    }
}
