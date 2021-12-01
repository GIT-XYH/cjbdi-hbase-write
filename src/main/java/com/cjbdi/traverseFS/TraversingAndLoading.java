package com.cjbdi.traverseFS;

import com.cjbdi.version3.HDFSDUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.ArrayList;

/**
 * @Author: XYH
 * @Date: 2021/11/16 10:55 上午
 * @Description: 遍历并下载文件系统下的所有文件夹
 */
public class TraversingAndLoading {
    public void traversing(String path) {
        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (null == files || files.length == 0) {
                System.out.println("文件夹是空的!");
                return;
            }else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        System.out.println("文件夹: " + file2.getAbsolutePath());
                        traversing(file2.getAbsolutePath());
                    } else {
                        System.out.println("文件: " + file2.getAbsolutePath());
                    }
                }
            }
        }else {
            System.out.println("文件不存在");
        }
    }

}