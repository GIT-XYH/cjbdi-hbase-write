package com.cjbdi.version5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;

/**
 * @Author: XYH
 * @Date: 2021/11/11 10:42 上午
 * @Description: 想 HBase 表中 put 数据
 */
public class HBasePutData {
    static Configuration conf = null;
    static Connection conn = null;
    static {
        conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "rookiex01,rookiex02,rookiex03");
        conf.set("hbase.zookeeper.quorum", "bd-01");
        conf.set("hbase.zookeeper.property.client", "2181");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        conf.set("hbase.client.keyvalue.maxsize","102400000");
        try{
            conn = ConnectionFactory.createConnection(conf);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //向表中添加数据(多个 rowKey, 多个列族)
    public static void insertData() throws Exception {
//        Table table = conn.getTable(TableName.valueOf("test:t1"));
        Table table = conn.getTable(TableName.valueOf("ns_ws:t_ws_test"));
        ArrayList<Put> puts = new ArrayList<>();
        Configuration conf = new Configuration();

//        //rowKey 设计
//        String rowKey = new Date().getTime() + "";
//        System.out.println("rowKey 为: " + rowKey);
//        //指定ROWKEY的值
//        Put put = new Put(Bytes.toBytes(rowKey));
//        put.addColumn(Bytes.toBytes("doc_content"), Bytes.toBytes("binary"), Bytes.toBytes("wd"));
//
////        Put put2 = new Put(Bytes.toBytes("rk002"));
////        put2.addColumn(Bytes.toBytes("ws_xx"), Bytes.toBytes("age"), Bytes.toBytes("25"));
//
//        puts.add(put);
////        puts.add(put2);
//        table.put(puts);
//
//        table.close();
//        System.out.println("插入成功");
    }

    public static void main(String[] args) throws Exception {
        HBasePutData.insertData();
    }
}
