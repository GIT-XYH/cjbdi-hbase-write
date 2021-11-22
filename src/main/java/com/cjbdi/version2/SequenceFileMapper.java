package com.cjbdi.version2;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
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
public class SequenceFileMapper extends Mapper<Text, BytesWritable, BytesWritable, Put> {

    static {
        System.out.println("我要执行!!!!");
    }
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("/tmp/xyh/seqFile"), conf, "hdfs");
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        System.out.println("我执行了!!!");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("/tmp/xyh/picSequenceFile"), conf);
        while (reader.next(key, value)) {
            String rowKey = new Date().getTime() + "";
            System.out.println("rowKey 为: " + rowKey);
            //指定 rowkey 的值
            Put put = new Put(Bytes.toBytes(rowKey));
            BytesWritable bytesWritable = new BytesWritable(Bytes.toBytes(rowKey));        //行健
            put.addColumn("pic_content".getBytes(), "pic".getBytes(), value.getBytes());
            context.write(bytesWritable, put);
        }

    }

}
