package com.cjbdi.version0;

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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;
import java.util.Date;

public class SequenceFileTest {
    //HDFS路径
    String inpath = null;
    String outpath = null;
    SequenceFile.Writer writer = null;
    Table table = null;
    Configuration hbaseConf = null;
    Connection conn = null;

    //"/fayson/picHbase";
    //"/fayson/out";
    SequenceFileTest(String inpath, String outpath) {
        this.inpath = inpath;
        this.outpath = outpath;
    }

    //"picHbase"
    public void initHbase(String tableName) throws IOException {
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "bd-01");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure");
        conn = ConnectionFactory.createConnection(hbaseConf);
        table = (Table) conn.getTable(TableName.valueOf(tableName));
    }

    public void test() throws Exception {
        URI uri = new URI(inpath);
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(uri, conf,"hdfs");
        //实例化writer对象
        writer = SequenceFile.createWriter(fileSystem, conf, new Path(outpath), Text.class, BytesWritable.class);

        //递归遍历文件夹，并将文件下的文件写入sequenceFile文件
        listFileAndWriteToSequenceFile(fileSystem,inpath);

        //关闭流
        org.apache.hadoop.io.IOUtils.closeStream(writer);

        //读取所有文件
        URI seqURI = new URI(outpath);
        FileSystem fileSystemSeq = FileSystem.get(seqURI, conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystemSeq, new Path(outpath), conf);

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
            put.addColumn("ws_xx".getBytes(), "content".getBytes() , val.getBytes());
            table.put(put);
        }
        table.close();
        org.apache.hadoop.io.IOUtils.closeStream(reader);

    }

    /****
     * 递归文件;并将文件写成SequenceFile文件
     * @param fileSystem
     * @param path
     * @throws Exception
     */
    public void listFileAndWriteToSequenceFile(FileSystem fileSystem, String path) throws Exception{
        final FileStatus[] listStatuses = fileSystem.listStatus(new Path(path));

        for (FileStatus fileStatus : listStatuses) {
            if(fileStatus.isFile()){
                Text fileText = new Text(fileStatus.getPath().toString());
                System.out.println(fileText.toString());
                //返回一个SequenceFile.Writer实例 需要数据流和path对象 将数据写入了path对象
                FSDataInputStream in = fileSystem.open(new Path(fileText.toString()));
                byte[] buffer = IOUtils.toByteArray(in);
                in.read(buffer);
                BytesWritable value = new BytesWritable(buffer);

                //写成SequenceFile文件
                //文件名, 二进制内容
                writer.append(fileText, value);
            }
            if(fileStatus.isDirectory()){
                listFileAndWriteToSequenceFile(fileSystem, fileStatus.getPath().toString());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        SequenceFileTest sequenceFileTest = new SequenceFileTest("/tmp/xyh_test/图片测试.png", "/tmp/xyh_test/res2");
        sequenceFileTest.initHbase("ns_ws:t_ws_test");
        sequenceFileTest.test();
    }
}