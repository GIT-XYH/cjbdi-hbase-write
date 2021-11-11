package com.cjbdi.hdfsTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;
/**
 * @Author: XYH
 * @Date: 2021/11/10 5:23 下午
 * @Description: 将本地文件通过 java 客户端上传的奥 Hdfs, 测试连通性
 */

    public class HDFSPut {
        public static void main(String[] args) throws Exception, Exception {

            //获取 hdfs 的客户端对象
            URI uri = new URI("hdfs://rookiex01:8020/");
            Configuration conf = new Configuration();
            // 物理切块64M
            conf.set("dfs.blocksize", "64M");
            // 存储副本的个数
            conf.set("dfs.replication", "2");

            FileSystem fs = FileSystem.newInstance(uri, conf, "root");            System.out.println("正在上传.....");

            Path f1 = new Path("/Users/xuyuanhang/Desktop/temp/a.txt");
            Path f2 = new Path("/Users/xuyuanhang/Desktop/temp/b.txt");
            Path f3 = new Path("/Users/xuyuanhang/Desktop/temp/c.txt");

            // 参数三 是一个path的数组  可以同时上传多个文件和文件夹
            fs.copyFromLocalFile(true , true , new Path[] {f1,f2,f3}, new Path("/"));
            System.out.println("上传完毕.....");

            // 3 释放资源
            fs.close();
        }

    }
