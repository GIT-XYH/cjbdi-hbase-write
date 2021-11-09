package com.cjbdi.version0;
/*
在HDFS目录/tmp/person.txt中，准备数据源如下：
1 smartloli 100
2 smartloli2 101
3 smartloli3 102
*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.Date;

/**
 * Read DataSource from hdfs & Generator hfile.
 * @author smartloli.
 * Created by Aug 19, 2018
 */
public class GeneratorHFile2 {
    //
    //  文件导入Mapper
    //  静态类
    //
    static class HFileImportMapper2 extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        protected final String CF_KQ = "cf";      //列族名称
        private final String[] nameTable = { "id", "name", "No." };

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();        //得到字符串
            System.out.println("line : " + line);
            String[] datas = line.split(" ");
            String row = new Date().getTime() + "_" + datas[1];

            Put put = new Put(Bytes.toBytes(row));
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(row));        //行健
            for (int i = 0; i < datas.length; i++) {      //对整个字符串数组进行遍历
                put.addColumn(Bytes.toBytes(CF_KQ), Bytes.toBytes(nameTable[i]), Bytes.toBytes(datas[i]));
            }
            context.write(rowkey, put);            //利用context进行写
        }
    }

    public void generateFile() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "bd-01");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        Connection conn = null;

        //config.set("hbase.fs.tmp.dir", "partitions_" + UUID.randomUUID());    //临时目录
        String tableName = "t_person";
        String input = "hdfs://bd-01:8020/yfx_test/tmp/person.txt";
        String output = "hdfs://bd-01:8020/yfx_test/tmp/pres";                  //输出目录
        final Path outputPath = new Path(output);
        System.out.println("table : " + tableName);                             //表名
        Table table;
        try {
            try {
                //
                // org.apache.hadoop.fs.FileSystem
                //
                FileSystem fs = FileSystem.get(URI.create(output), config);       //根据路径创建文件系统
                fs.delete(new Path(output), true);
                fs.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }

            conn = ConnectionFactory.createConnection(config);                  //根据配置， 创建一个连解
            table = (Table) conn.getTable(TableName.valueOf(tableName));

            Job job = Job.getInstance(config);
            job.setJobName("Generate HFile");                                  //配置Job名
            job.setJarByClass(GeneratorHFile2.class);                          //根据class来设置job中相应的jar
            job.setMapperClass(HFileImportMapper2.class);                      //设置Mapper类，静态类
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);

            //设置文件的输入路径和输出路径
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(HFileOutputFormat2.class);
            FileInputFormat.setInputPaths(job, input);                //文件输入格式
            FileOutputFormat.setOutputPath(job, new Path(output));    //文件输出格式

            RegionLocator regionLocator = conn.getRegionLocator(TableName.valueOf(tableName));
            HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
            try {
                job.waitForCompletion(true);
                LoadIncrementalHFiles load = new LoadIncrementalHFiles(config);
                Admin admin=conn.getAdmin();
                load.doBulkLoad(outputPath, admin, table, regionLocator);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}