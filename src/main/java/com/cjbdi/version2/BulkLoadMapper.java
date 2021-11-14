package com.cjbdi.version2;


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

    protected final String CF_KQ = "doc_content";            //列族名称
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String skey = "abcdefg123456789";
        Put put = new Put(Bytes.toBytes(skey));
        ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(skey));        //行健
        put.addColumn(Bytes.toBytes(CF_KQ), Bytes.toBytes("mesgRaw"), value.getBytes());
        context.write(rowkey, put);  //利用context进行写
    }
}
