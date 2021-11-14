package com.cjbdi.myversion;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author: XYH
 * @Date: 2021/11/7 8:50 上午
 * @Description: 利用多线程将 HFile 文件加载到 HBase 中
 */
public class MyLoadData {

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "rookiex01,rookiex02,rookiex03");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
//        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");

        // 创建线程池
//        ExecutorService service = Executors.newFixedThreadPool(10);
//        Runnable runnable = new Runnable(){
//            @Override
//            public void run() {
//                try {
                    //获取数据库连接
                    Connection connection = ConnectionFactory.createConnection(configuration);
                    //获取表的管理器对象
                    Admin admin = connection.getAdmin();
                    //获取table对象
                    TableName tableName = TableName.valueOf("ns_ws:t_ws_test");
                    Table table = connection.getTable(tableName);

                    MyHBaseBulkLoad.bulkLoad(args);
                    //构建LoadIncrementalHFiles加载HFile文件
                    LoadIncrementalHFiles load = new LoadIncrementalHFiles(configuration);
                    load.doBulkLoad(new Path("hdfs://rookiex01:8020/xyh/pic_outhfile"), admin, table, connection.getRegionLocator(tableName));
                    try {
                        FileSystem fs = FileSystem.get(new URI("hdfs://rookiex01:8020"), configuration);
                        fs.delete(new Path("/xyh/pic_outhfile"));
                        fs.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                System.out.println("任务被执行, 线程" + Thread.currentThread().getName());
//            }
//        };
//        for (int i = 0; i < 10; i++) {
//        service.execute(runnable);
//        }
        long endTime = System.currentTimeMillis();
        System.out.println("操作共耗时: " + (endTime-startTime) + "毫秒");
//        service.shutdown();

    }
}
