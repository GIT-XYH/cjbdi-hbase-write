package com.cjbdi.myversion;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.Date;


/**
 * @Author: XYH
 * @Date: 2021/11/6 5:58 下午
 * @Description: 自定义 map 类
 */
public class MyBulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{

    protected final String CF_KQ = "ws_xx";            //列族名称
    String inpath = null;
    String outpath = null;
    SequenceFile.Writer writer = null;
    Table table = null;
    Configuration hbaseConf = null;
    Connection conn = null;

    @Override
    protected void map(LongWritable key, Text value, Context context) {
//        String skey = "abcdefg123456789";
//        Put put = new Put(Bytes.toBytes(skey));
//        ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(skey));        //行健
//        put.addColumn(Bytes.toBytes(CF_KQ), Bytes.toBytes("mesgRaw"), value.getBytes());
//        context.write(rowkey, put);  //利用context进行写
        String row = new Date().getTime() + "";
        System.out.println(row);
        //封装输出的rowkey类型
        ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(row));

//        Put put = new Put(value.getBytes() );
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(CF_KQ.getBytes(), "binary_pic".getBytes(), value.getBytes());
//
    }
}
