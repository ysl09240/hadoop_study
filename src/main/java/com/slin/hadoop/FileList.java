package com.slin.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
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
public class FileList {
    public static void main(String[] args) throws IOException {
        if(args.length != 1){
            System.err.println("Usage:filelist<source>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[0]),conf);
        FileStatus[] status = fs.listStatus(new Path(args[0]));
        Path[] listedPaths = FileUtil.stat2Paths(status);

        for(Path p :listedPaths){
            System.out.println(p);
        }
    }
}
