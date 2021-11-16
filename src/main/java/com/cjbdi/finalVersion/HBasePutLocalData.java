package com.cjbdi.finalVersion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.FileInputStream;

/**
 * @Author: XYH
 * @Date: 2021/11/11 10:42 上午
 * @Description: 从 Linux 本地向 HBase 表中 put 数据
 */
public class HBasePutLocalData {
    static Configuration conf = null;
    static Connection conn = null;
    static {
        conf = HBaseConfiguration.create();
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
        //获取需要添加数据的表
        Table table = conn.getTable(TableName.valueOf("ns_xyh:t_doc"));
        //指定本地添加路径
        String path = "/data/xyh/doc/50M测试文件1.doc";
        FileInputStream fis = new FileInputStream(path);
        //读图为流, 但是字节数组还是空
        byte[] bbb = new byte[fis.available()];
        //将文件内容写入字节数组
        fis.read(bbb);
        fis.close();
        //002是 rowKey
        Put put = new Put("002".getBytes());
        //pic_content 是列族, img 是列, bbb 是插入的值(图片转换为字节数组)
        put.addColumn("doc_content".getBytes(),"doc".getBytes(),bbb);
        table.put(put);
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
        HBasePutLocalData.insertData();
        long endTime = System.currentTimeMillis();
        System.out.println("HBase get 数据共耗时: " + (endTime-startTime));
    }
}
