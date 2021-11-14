package com.cjbdi.version1;


import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Date;


/**
 * @Author: XYH
 * @Date: 2021/11/6 5:58 下午
 * @Description: 自定义 mapper类, 对于结构化文本, 可分割的
 */
public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\\s+");

        //封装输出的rowkey类型
        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(split[0].getBytes());
        //为指定的行创建一个 put 操作
        Put put = new Put(split[0].getBytes());
        put.addColumn("txt_content".getBytes(), "no".getBytes(), split[0].getBytes());
        put.addColumn("txt_content".getBytes(), "name".getBytes(), split[1].getBytes());
        put.addColumn("txt_content".getBytes(), "score".getBytes(), split[2].getBytes());

        //使用 context 进行写操作
        context.write(immutableBytesWritable, put);
    }
}
