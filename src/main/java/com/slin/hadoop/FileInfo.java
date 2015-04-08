package com.slin.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
public class FileInfo {
    public static void main(String[] args) throws IOException {
        if(args.length != 1){
            System.err.println("Usage:fileinfo<source>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[0]),conf);
        FileStatus stat = fs.getFileStatus(new Path(args[0]));

        System.out.println(stat.getPath());
        System.out.println(stat.getLen());
        System.out.println(stat.getModificationTime());
        System.out.println(stat.getOwner());
        System.out.println(stat.getReplication());
        System.out.println(stat.getBlockSize());
        System.out.println(stat.getGroup());
        System.out.println(stat.getPermission().toString());







    }
}
