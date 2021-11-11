package com.cjbdi.hdfsTest;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @Author: XYH
 * @Date: 2021/11/10 5:15 下午
 * @Description: 测试在 Hdfs 上创建目录
 */
public class HDFSMkdirs {
        public static void main(String[] args) throws Exception {
            // 创建文件夹(多级)
            FileSystem fs = HDFSUtils.getFs();
            fs.mkdirs(new Path("/xxx")) ;
            fs.close();
        }
}
