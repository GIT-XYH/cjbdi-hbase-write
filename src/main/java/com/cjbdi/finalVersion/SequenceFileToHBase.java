package com.cjbdi.finalVersion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

/**
 * @Author: XYH
 * @Date: 2021/11/21 8:16 下午
 * @Description: 将 sequenceFile 加载到hbase 中
 */
public class SequenceFileToHBase {
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "bd-01,bd-02,bd-03");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure");
        hbaseConf.set("hbase.client.keyvalue.maxsize", "102400000");
        Connection conn = ConnectionFactory.createConnection(hbaseConf);
        Table table = (Table) conn.getTable(TableName.valueOf("ns_xyh:t_pic"));
        Configuration conf = new Configuration();
        URI seqURI = new URI("/tmp/xyh/docSequenceFile");
        FileSystem fileSystemSeq = FileSystem.get(seqURI, conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystemSeq, new Path("/tmp/xyh/docSequenceFile"), conf);
        // Sequence File的键值对
        Text key = new Text();
        BytesWritable val = new BytesWritable();
        //key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        //val = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

        // 把sequenceFile中的内容写到hbase中
        int i = 0;
        while(reader.next(key, val)){
            String temp = key.toString();
            temp = temp.substring(temp.lastIndexOf("/") + 1);
            //rowKey 设计
            String rowKey = new Date().getTime() + "";
            System.out.println("rowKey 为: " + rowKey);
            //指定ROWKEY的值
            Put put = new Put(Bytes.toBytes(rowKey));
            //指定列簇名称、列修饰符、列值 temp.getBytes()
            put.addColumn("pic_content".getBytes(), "pic".getBytes() , val.getBytes());
            table.put(put);
        }
        table.close();
        org.apache.hadoop.io.IOUtils.closeStream(reader);
        long endTime = System.currentTimeMillis();
        System.out.println("操作共耗时: " + (endTime-startTime)/1000 + "秒");
    }
}
