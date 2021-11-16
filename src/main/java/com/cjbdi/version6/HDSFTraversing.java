package com.cjbdi.version6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @Author: XYH
 * @Date: 2021/11/16 10:55 上午
 * @Description: 遍历读取 hdfs 目录下的所有文件
 */
public class HDSFTraversing {
    public static void main(String[] args) throws Exception {
        FileSystem fs = HDFSDUtils.getFs();
        //遍历所有文件和文件夹
        FileStatus[] fss = fs.listStatus(new Path("/tmp/xyh/doc"));
        for (FileStatus fileStatus : fss) {
            //fileStatus 路径下所有的文件或者文件夹
            if (fileStatus.isDirectory()) {
                Path path = fileStatus.getPath();
                System.out.println(path);
            } else {
                //获取文件路径
                System.out.println(fileStatus.getPath());
                //获取文件名
//                System.out.println(fileStatus.getPath().getName());
            }
        }
        fs.close();
    }
}