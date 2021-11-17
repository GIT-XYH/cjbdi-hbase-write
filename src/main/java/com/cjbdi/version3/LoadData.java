package com.cjbdi.version3;

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
public class LoadData {

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "bd-01");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");

        //获取数据库连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取表的管理器对象
        Admin admin = connection.getAdmin();
        //获取table对象
        TableName tableName = TableName.valueOf("ns_xyh:t_doc");
        Table table = connection.getTable(tableName);
        // 创建线程池
        ExecutorService service = Executors.newFixedThreadPool(5);
        Runnable runnable = new Runnable(){
            @Override
            public void run() {
                try {

                    HBaseBulkLoad.bulkLoad(args);
                    //构建LoadIncrementalHFiles加载HFile文件
                    LoadIncrementalHFiles load = new LoadIncrementalHFiles(configuration);
                    load.doBulkLoad(new Path("hdfs://bd-01:8020/tmp/xyh/doc_out4"), admin, table, connection.getRegionLocator(tableName));
                    try {
                        FileSystem fs = FileSystem.get(new URI("hdfs://bd-01:8020"), configuration);
                        fs.delete(new Path("/tmp/xyh/doc_out4"));
                        fs.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("任务被执行, 线程" + Thread.currentThread().getName());
            }
        };
        for (int i = 0; i < 10; i++) {
        service.execute(runnable);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("操作共耗时: " + (endTime-startTime) + "毫秒");
        service.shutdown();

    }
}
