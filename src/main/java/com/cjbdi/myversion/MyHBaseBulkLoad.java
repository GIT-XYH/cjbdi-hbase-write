package com.cjbdi.myversion;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;


/**
 * @Author: XYH
 * @Date: 2021/11/8 10:13 上午
 * @Description: 将 HDFS 上的文件生成 HFile 并存储在 hdfs 上
 */
public class MyHBaseBulkLoad extends Configured implements Tool {
    public static void bulkLoad(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        //设置ZK集群
        configuration.set("hbase.zookeeper.quorum", "rookiex01,rookiex02,rookiex03");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
//        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
        ToolRunner.run(configuration, new MyHBaseBulkLoad(), args);
//    }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MyHBaseBulkLoad.class);

//        FileInputFormat.addInputPath(job, new Path("hdfs://rookiex01:8020/xyh/pic/pic1.jpeg"));
        SequenceFileInputFormat.addInputPath(job, new Path("hdfs://rookiex01:8020/xyh/picSeq"));
        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setMapperClass(MyBulkLoadMapper.class);


        //获取数据库连接
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("ns_ws:t_ws_test"));

        //使MR可以向表中，增量增加数据
        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(TableName.valueOf("ns_ws:t_ws_test")));
        //数据写回到HDFS，写成HFile -> 所以指定输出格式为HFileOutputFormat2
        job.setOutputFormatClass(HFileOutputFormat2.class);
        String time = new Date().getTime() + "";
//        Path path = new Path("hdfs://rookiex01:8020/xyh/pic_out");
        Path path = new Path("hdfs://rookiex01:8020/xyh/picHfile");
        HFileOutputFormat2.setOutputPath(job, path);

        //开始执行
        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

}
