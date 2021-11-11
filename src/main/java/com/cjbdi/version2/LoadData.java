package com.cjbdi.version2;

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

import java.net.URI;

/**
 * @Author: XYH
 * @Date: 2021/11/7 8:50 上午
 * @Description: 将 HFile 文件加载到 HBase 中
 */
public class LoadData {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.quorum", "bd-01,bd-02,bd-03");
        configuration.set("hbase.zookeeper.quorum", "bd-01");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");

        //获取数据库连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取表的管理器对象
        Admin admin = connection.getAdmin();
        //获取table对象
        TableName tableName = TableName.valueOf("ns_ws:t_ws_test");
        Table table = connection.getTable(tableName);

        for (int i = 0; i < 3; i++) {
            HBaseBulkLoad.bulkLoad(args);
            //构建LoadIncrementalHFiles加载HFile文件
            LoadIncrementalHFiles load = new LoadIncrementalHFiles(configuration);
            load.doBulkLoad(new Path("hdfs://bd-01:8020/tmp/xyh_test/outHfile"), admin, table, connection.getRegionLocator(tableName));
            FileSystem fs = FileSystem.get(new URI("hdfs://bd-01:8020"), configuration);
            fs.delete(new Path("/tmp/xyh_test/outHfile"));
        }
    }
}
