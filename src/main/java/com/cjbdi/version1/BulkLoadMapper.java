package com.cjbdi.version1;


import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * @Author: XYH
 * @Date: 2021/11/6 5:58 下午
 * @Description: 自定义 mapper类
 */
public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\\s+");

        //封装输出的rowkey类型
        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(split[0].getBytes());
        //构建Put对象
        Put put = new Put(split[0].getBytes());
        put.addColumn("ws_xx".getBytes(), "no".getBytes(), split[1].getBytes());
        put.addColumn("ws_xx".getBytes(), "city".getBytes(), split[2].getBytes());
//        put.addColumn("ws_xx".getBytes(), "sco".getBytes(), split[3].getBytes());

        context.write(immutableBytesWritable, put);
    }
}
