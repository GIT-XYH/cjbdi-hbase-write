package com.cjbdi.myversion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.FileOutputStream;

/**
 * @Author: XYH
 * @Date: 2021/11/11 11:24 上午
 * @Description: 从 HBase 中 get 数据, 根据 rowkey 查询数据
 */
public class MyHBaseGetData {

    static Configuration conf = null;
    static Connection conn = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "rookiex01,rookiex02,rookiex03");
        conf.set("hbase.zookeeper.property.client", "2181");
//        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        try{
            conn = ConnectionFactory.createConnection(conf);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void getResult(TableName tableName, String rowKey) throws Exception{
        Table table = conn.getTable(tableName);
        //获得一行
        Get get = new Get(Bytes.toBytes(rowKey));
        Result rs = table.get(get);
        //保存 get result 的结果, 字节数组的形式
        byte[] bs = rs.value();
        table.close();
        File file = new File("/data/xxx");
        FileOutputStream fos = new FileOutputStream(file);
        fos.write(bs);
        fos.close();
    }


//    public static void scanTable(TableName tableName) throws Exception{
//        Table table = conn.getTable(tableName);
//        Scan scan = new Scan();
////        //可以做筛选
////        scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"));
////        scan.addFamily(Bytes.toBytes("f1"));
////        scan.setStartRow(scan.getStartRow());
////        scan.setStopRow(scan.getStopRow());
////        scan.setTimestamp(timestamp);
//
//        ResultScanner scanner = table.getScanner(scan);
//        for (Result result : scanner) {
//            Cell[] cells = result.rawCells();
//            for (Cell cell : cells) {
//                System.out.println("rowkey: " + Bytes.toString((CellUtil.cloneRow(cell))));
//                System.out.println("列族: " + Bytes.toString((CellUtil.cloneFamily(cell))));
//                System.out.println("列: " + Bytes.toString((CellUtil.cloneQualifier(cell))));
//                System.out.println("值: " + Bytes.toString((CellUtil.cloneValue(cell))));
//                System.out.println("时间戳: " + cell.getTimestamp());
//                System.out.println("*********************************************");
//            }
//        }
//    }
public static void main(String[] args) throws Exception {
    MyHBaseGetData.getResult(TableName.valueOf("ns_ws:t_ws_test"), "abcdefg123456789");
//    MyHBaseGetData.scanTable(TableName.valueOf("ns_xyh:t_pic"));
}

}
