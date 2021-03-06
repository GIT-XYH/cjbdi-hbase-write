package com.cjbdi.finalVersion;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.net.URI;

/**
 * @Author: XYH
 * @Date: 2021/11/14 6:31 下午
 * @Description:
 */
public class GeneratorSequenceFile {
    //HDFS路径
    String inpath = null;
    String outpath = null;
    SequenceFile.Writer writer = null;

    GeneratorSequenceFile(String inpath, String outpath) {
        this.inpath = inpath;
        this.outpath = outpath;
    }

    //递归文件, 将文件写成 sequenceFile
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
    public void test() throws Exception {
        URI uri = new URI(inpath);
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(uri, conf, "hdfs");
        //实例化writer对象
        writer = SequenceFile.createWriter(fileSystem, conf, new Path(outpath), Text.class, BytesWritable.class);
        //递归遍历文件夹，并将文件下的文件写入sequenceFile文件
        listFileAndWriteToSequenceFile(fileSystem, inpath);
        //关闭流
        org.apache.hadoop.io.IOUtils.closeStream(writer);
        //读取所有文件
        URI seqURI = new URI(outpath);
        FileSystem fileSystemSeq = FileSystem.get(seqURI, conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystemSeq, new Path(outpath), conf);
        // Sequence File的键值对
        Text key = new Text();
        BytesWritable val = new BytesWritable();
    }

    public static void main(String[] args) throws Exception{
        GeneratorSequenceFile sequenceFileTest = new GeneratorSequenceFile("/tmp/xyh/doc_all", "/tmp/xyh/docSequenceFile");
        sequenceFileTest.test();

    }
}
