package com.cjbdi.version3;


import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Date;


/**
 * @Author: XYH
 * @Date: 2021/11/6 5:58 下午
 * @Description: 自定义 map 类
 */
public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = new Date().getTime() + "";
        System.out.println("rowkey 为: " + row);
        //封装输出的rowkey类型
        ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(row));

//        Put put = new Put(value.getBytes() );
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn("doc_content".getBytes(), "binary_doc".getBytes(), value.getBytes());
        context.write(rowkey, put);
    }
}
