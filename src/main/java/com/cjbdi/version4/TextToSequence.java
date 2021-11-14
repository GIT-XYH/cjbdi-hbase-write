package com.cjbdi.version4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
/**
 * @Author: XYH
 * @Date: 2021/11/14 4:45 下午
 * @Description: 将二进制文本转换为 SequenceFile
 */
public class TextToSequence {
    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(TextToSequence.class);

        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VLongWritable.class);

        // 设置输出类
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        /**
         * 设置sequecnfile的格式，对于sequencefile的输出格式，有多种组合方式,
         * 从下面的模式中选择一种，并将其余的注释掉
         */

        // 组合方式1：不压缩模式
        SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.NONE);

        //组合方式2：record压缩模式，并指定采用的压缩方式 ：默认、gzip压缩等
        //        SequenceFileOutputFormat.setOutputCompressionType(job,
        //                CompressionType.RECORD);
        //        SequenceFileOutputFormat.setOutputCompressorClass(job,
        //                DefaultCodec.class);


        //组合方式3：block压缩模式，并指定采用的压缩方式 ：默认、gzip压缩等
        //        SequenceFileOutputFormat.setOutputCompressionType(job,
        //                CompressionType.BLOCK);
        //        SequenceFileOutputFormat.setOutputCompressorClass(job,
        //                DefaultCodec.class);

        FileInputFormat.addInputPaths(job, "hdfs://fz/user/hdfs/MapReduce/data/squenceFile/origin");
        SequenceFileOutputFormat.setOutputPath(job, new Path("hdfs://fz/user/hdfs/MapReduce/data/squenceFile/textToSequence/output"));

        System.exit(job.waitForCompletion(true)?0:1);
    }
    //map
    public static class WCMapper extends
            Mapper<LongWritable, Text, Text, VLongWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] split = value.toString().split("");
            for(String s : split){
                context.write(new Text(s), new VLongWritable(1L));
            }
        }
    }
    //reduce
    public static class WCReducer extends Reducer<Text, VLongWritable, Text, VLongWritable>{
        @Override
        protected void reduce(Text key, Iterable<VLongWritable> v2s, Context context)
                throws IOException, InterruptedException {

            long sum = 0;

            for (VLongWritable vl : v2s) {
                sum += vl.get();
            }
            context.write(key, new VLongWritable(sum));

        }
        }
}
