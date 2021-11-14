package com.cjbdi.version5;

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
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @Author: XYH
 * @Date: 2021/11/12 11:37 上午
 * @Description: 使用 BulkLoad 的方式将 SequenceFile 存入 HBase
 */
public class SequenceFileWithBulkLoad {

    String inpath = null;
    String outpath = null;
    SequenceFile.Writer writer = null;
    Table table = null;
    Configuration hbaseConf = null;
    Connection conn = null;


    SequenceFileWithBulkLoad(String inpath, String outpath) {
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

    //递归文件, 将文件生成 SequenceFile
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

}
