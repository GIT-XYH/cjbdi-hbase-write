package com.cjbdi.version0;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class KafkaDemo {
    private static final String topic = "my-topic";
    private static final String service = "bd-01:6667";

    public static void main(String[] args) throws IOException {
        String str=null;

        hdemo.HbaseTest hbaseTest = new hdemo.HbaseTest();
        hbaseTest.init();

        //str = getHbase() + "--文书内容";
        str = hbaseTest.getCellByRowkey("t_ws","00297405af4a4dc09cadad19008f68cb","wsxx", "c_nrtxt") + "--文书内容";

        //
        // added by yfx later
        //
        //hbaseTest.insertData();
        //putHbase();
        //GeneratorHFile2 generatorHFile2 = new GeneratorHFile2();
        //generatorHFile2.generateFile();
        hdemo.BulkLoad bulkLoad = new hdemo.BulkLoad();
        bulkLoad.generateFile();

        //kafka生产者
        send2kafka(str);
        consumer();

    }

    private static void consumer() {
        //配置信息
        Properties props = new Properties();
        //kafka服务器地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, service);
        //必须指定消费者组
        props.put("group.id", "test");
        //设置数据key和value的序列化处理类
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);
        //创建消息者实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅topic
        consumer.subscribe(Collections.singletonList(topic));
        //到服务器中读取记录
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("key:" + record.key() + "" + ",value:" + record.value());
            }
        }
    }

    private static void send2kafka(String str) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, service);//kafka地址，多个地址用逗号分割
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        String msg = UUID.randomUUID().toString();
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg, str);
                        kafkaProducer.send(record);
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();

    }

    private static String getHbase() {
        String res = "";
        Connection connection = null;
        try {
            org.apache.hadoop.conf.Configuration config = null;// 配置
            config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "bd-01");
            config.set("hbase.zookeeper.property.clientPort", "2181");
            config.set("zookeeper.znode.parent", "/hbase-unsecure");
            connection = ConnectionFactory.createConnection(config);
            Table table = (Table) connection.getTable(TableName.valueOf("t_ws"));
            Get get = new Get(Bytes.toBytes("00297405af4a4dc09cadad19008f68cb")); // 通过rowkey创建一个 get 对象
            get.addColumn(Bytes.toBytes("wsxx"), Bytes.toBytes("c_nrtxt"));
            Result result = table.get(get);
            res = new String(result.getValue(Bytes.toBytes("wsxx"), Bytes.toBytes("c_nrtxt")));
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return res;
    }

    private static String putHbase() {
        String res = "";
        Connection connection = null;
        try {
            org.apache.hadoop.conf.Configuration config = null;// 配置
            config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "bd-01");
            config.set("hbase.zookeeper.property.clientPort", "2181");
            config.set("zookeeper.znode.parent", "/hbase-unsecure");
            config.set("hbase.client.keyvalue.maxsize","52428800");
            connection = ConnectionFactory.createConnection(config);

            Table table = (Table) connection.getTable(TableName.valueOf("t_ws_test_1"));

            String srcFilePath = "./test_file/xaa";
            FileInputStream fis = new FileInputStream(srcFilePath);
            //byte[] byteBuffer = new byte[fis.available() -50];//读图为流，但字节数组还是空的
            byte[] byteBuffer = new byte[fis.available()];
            int iLen = fis.read(byteBuffer);//将文件内容写入字节数组
            fis.close();
            System.out.printf("read %d bytes this time\n", iLen);

            long begin = System.currentTimeMillis();
            System.out.printf("begin time:%d.\n", begin);
            for ( int i =0; i < 5000; i++) {
                String rowkey = "rowkey_10_" + String.valueOf(i);;
                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes("ws_xx"), Bytes.toBytes("username"), Bytes.toBytes("张三"));
                put.addColumn(Bytes.toBytes("ws_xx"), Bytes.toBytes("sex"), Bytes.toBytes("1"));
                put.addColumn(Bytes.toBytes("ws_xx"), Bytes.toBytes("address"), Bytes.toBytes("北京市"));
                put.addColumn(Bytes.toBytes("ws_xx"), Bytes.toBytes("birthday"), Bytes.toBytes("2014-07-10"));
                put.addColumn(Bytes.toBytes("ws_xx"), Bytes.toBytes("c_nrsrc"), byteBuffer);
                put.addColumn(Bytes.toBytes("ws_xx"), Bytes.toBytes("c_nrtxt"), Bytes.toBytes("text content of file"));
                table.put(put);
            }
            long end = System.currentTimeMillis();
            System.out.printf("begin time:%d.\n", end);
            System.out.println("spend time:");
            System.out.println((end - begin)/1000);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return res;
    }
}
