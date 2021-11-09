package com.cjbdi.version1;


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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @Author: XYH
 * @Date: 2021/11/6 10:13 下午
 * @Description: 将 HDFS 上的文本 txt文件生成 HFile 并存储在 hdfs 上
 * 	1 beijing 100
 * 	2 shanghai 101
 * 	3 tianjin 102
 */
public class HBaseBulkLoad extends Configured implements Tool {
//    public static void main(String[] args) throws Exception {
    public static void LoadHfile(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        //设置ZK集群
        configuration.set("hbase.zookeeper.quorum", "bd-01");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");

        int run = ToolRunner.run(configuration, new HBaseBulkLoad(), args);
//        System.exit(run);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(HBaseBulkLoad.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://bd-01:8020/tmp/xyh_test/person.txt"));
        job.setMapperClass(BulkLoadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //获取数据库连接
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("ns_ws:t_ws_test"));

        //使MR可以向表中，增量增加数据
        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(TableName.valueOf("ns_ws:t_ws_test")));
        //数据写回到HDFS，写成HFile -> 所以指定输出格式为HFileOutputFormat2
        job.setOutputFormatClass(HFileOutputFormat2.class);
        HFileOutputFormat2.setOutputPath(job, new Path("hdfs://bd-01:8020/tmp/xyh_test/outHfile1"));

        //开始执行
        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

}
