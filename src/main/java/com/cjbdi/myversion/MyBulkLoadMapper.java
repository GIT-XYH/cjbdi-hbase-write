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
import java.net.URISyntaxException;
import java.util.Date;


/**
 * @Author: XYH
 * @Date: 2021/11/6 5:58 下午
 * @Description: 自定义 map 类
 */
public class MyBulkLoadMapper extends Mapper<LongWritable, Text, BytesWritable, Put>{

    static {
        System.out.println("我要执行!!!!");
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("/xyh/picSeq"), conf, "root");
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        System.out.println("我执行了!!!");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("/xyh/picSeq"), conf);

        String temp = key.toString();
        temp = temp.substring(temp.lastIndexOf("/") + 1);
        while (reader.next(key, value)) {
            String rowKey = new Date().getTime() + "";
            System.out.println("rowKey 为: " + rowKey);
            //指定 rowkey 的值
            Put put = new Put(Bytes.toBytes(rowKey));
            BytesWritable text = new BytesWritable(Bytes.toBytes(rowKey));        //行健
            put.addColumn("pic_content".getBytes(), "pic".getBytes(), value.getBytes());
            context.write(text, put);
        }

    }
}
