package com.cjbdi.version5;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;

/**
 * @Author: XYH
 * @Date: 2021/11/16 10:55 上午
 * @Description: 遍历读取 hdfs 目录下的所有文件
 */
public class HDSFTraversing {
//    public static void main(String[] args) throws Exception {
    public static ArrayList<String> getPath() throws Exception{
        FileSystem fs = HDFSDUtils.getFs();
        //遍历所有文件和文件夹
        FileStatus[] fss = fs.listStatus(new Path("/tmp/xyh/doc"));
        ArrayList<String> strings = new ArrayList<>();
        for (FileStatus fileStatus : fss) {
            //fileStatus 路径下所有的文件或者文件夹
//            if (fileStatus.isDirectory()) {
//                Path path = fileStatus.getPath();
//                System.out.println(path);
//            } else {
//                获取文件路径
//                System.out.println(fileStatus.getPath());
//            }
            strings.add(fileStatus.getPath().toString());
        }
        return strings;
    }

}