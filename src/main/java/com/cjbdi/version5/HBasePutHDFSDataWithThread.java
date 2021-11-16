package com.cjbdi.version5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.FileInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Thread.sleep;

/**
 * @Author: XYH
 * @Date: 2021/11/11 10:42 上午
 * @Description: 想 HBase 表中 put 数据
 */
public class HBasePutHDFSDataWithThread {
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
        Table table = conn.getTable(TableName.valueOf("ns_xyh:t_doc"));
        ArrayList<String> path = HDSFTraversing.getPath();
        for (String s : path) {
            FileSystem fs = FileSystem.get(new URI(s), conf);
            //读图为流, 但是字节数组还是空
//            byte[] bbb = new byte[fis.available()];
//            //将文件内容写入字节数组
//            fis.read(bbb);
//            fis.close();
            //设置 rowKey
            String rowKey = new Date().getTime() + "";
            System.out.println("rowKey 为: " + rowKey);
            Put put = new Put(rowKey.getBytes());
            //pic_content 是列族, img 是列, bbb 是插入的值(图片转换为字节数组)
//            put.addColumn("pic_content".getBytes(),"img".getBytes(),bbb);
            table.put(put);
        }

        table.close();
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
        long startTime = System.currentTimeMillis();
        ExecutorService service = Executors.newFixedThreadPool(10);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    HBasePutHDFSDataWithThread.insertData();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        //连续存储两个小时
        while (System.currentTimeMillis() < startTime + 7200000) {
            service.execute(thread);
            sleep(1000);
        }
        service.shutdown();
    }
}
