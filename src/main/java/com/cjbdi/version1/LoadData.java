package com.cjbdi.version1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author: XYH
 * @Date: 2021/11/7 8:50 上午
 * @Description: 对小文本文件进行 bulkLoad 装载进 HBase 进行测试
 */
public class LoadData {
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "bd-01,bd-02,bd-03");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");

        //获取数据库连接
        Connection connection =  ConnectionFactory.createConnection(configuration);
        //获取表的管理器对象
        Admin admin = connection.getAdmin();
        //获取table对象
        TableName tableName = TableName.valueOf("ns_xyh:t_txt");
        Table table = connection.getTable(tableName);
        //生成 HFile
        HBaseBulkLoad.LoadHfile(args);
        //构建LoadIncrementalHFiles加载HFile文件
        LoadIncrementalHFiles load = new LoadIncrementalHFiles(configuration);
        //将 HFile 加载到HBase 指定表中
        load.doBulkLoad(new Path("hdfs://bd-01:8020/tmp/xyh/txt_out"), admin,table,connection.getRegionLocator(tableName));
        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://bd-01:8020"), configuration);
            fs.delete(new Path("/tmp/xyh/txt_out"));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("操作共耗时: " + (endTime-startTime) + "毫秒");

    }

}
