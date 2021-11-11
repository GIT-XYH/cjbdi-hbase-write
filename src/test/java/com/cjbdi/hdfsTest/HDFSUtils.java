package com.cjbdi.hdfsTest;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * @Author: XYH
 * @Date: 2021/11/10 5:23 下午
 * @Description: 获取 HDFS的客户端对象
 */

public class HDFSUtils {
    public static FileSystem  getFs() throws Exception {
        URI uri = new URI("hdfs://rookiex01:8020/");
        Configuration conf = new Configuration();
        // 配置信息
        //conf.set("dfs.blocksize", "64M");
        FileSystem fs = FileSystem.newInstance(uri, conf, "root");
        return fs ;
    }
}