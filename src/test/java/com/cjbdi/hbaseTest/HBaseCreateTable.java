package com.cjbdi.hbaseTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * @Author: XYH
 * @Date: 2021/11/11 10:12 上午
 * @Description: 在 HBase 中创建表
 */
public class HBaseCreateTable {
    public static void createTable() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "rookiex01");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        Connection conn = ConnectionFactory.createConnection(conf);

        Admin admin = conn.getAdmin();
        if (!admin.tableExists(TableName.valueOf("test"))){
            TableName tableName = TableName.valueOf("test");
            //表描述器构造器
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
            //列族描述器构造器
            ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("user"));
            //获得列描述器
            ColumnFamilyDescriptor cfd = cdb.build();
            //添加列族
            tdb.setColumnFamily(cfd);
            //获得表描述器
            TableDescriptor td = tdb.build();
            //创建表
            admin.createTable(td);
        }else {
            System.out.println("表已存在");
        }
        //关闭连接
        conn.close();
    }

    public static void main(String[] args) throws Exception {
        HBaseCreateTable.createTable();
    }
}
