package com.cjbdi.version0;

//
// 参考url: https://www.cnblogs.com/AlphaWA/p/11921901.html
//

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HbaseTest {
    private Configuration config = null;
    private Connection connection = null;

    public void init() throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "bd-01");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        connection = ConnectionFactory.createConnection(config);
    }

    //存储图片
    /**
     * @param tableName 表名
     * @param rowkey 行号
     * @param colFamily 列簇
     * @param colName 列名
     * @param bs 列值，此处为图片
     * @param imgType
     * 图片类型，读取图片时使用，图片类型的列名指定为imageType,imgType这里指的是列名为imageType的列值
     */
    public void storeImage(String tableName, String rowkey, String colFamily, String colName, byte[] bs, String imgType) throws IOException {

        Table table = (Table) connection.getTable(TableName.valueOf(tableName));
        List<Put> puts = new java.util.ArrayList<Put>();

        //
        //存二进制图片
        //
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colName), bs);

        //存图片类型
        Put putx = new Put(Bytes.toBytes(rowkey));
        putx.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("imageType"), Bytes.toBytes(imgType));

        puts.add(put);
        puts.add(putx);

        table.put(puts);
        table.close();
    }

    //
    //读取图片
    // 参阅 https://blog.csdn.net/m0_48624662/article/details/107403503
    //
    public  void getImage(String tableName, String rowkey) throws IOException {
        Table table = (Table) connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowkey));
        //get.addColumn(Bytes.toBytes("wsxx"), Bytes.toBytes("c_nrtxt"));
        Result result  = table.get(get);
        List<Cell> cs = result.listCells();

        for (Cell cell : cs) {
            byte[] row = CellUtil.cloneRow(cell);
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);
            System.out.println(new String(row)+"--"+new String(qualifier)+"--"+new String(value));
        }
        table.close();
    }

    //创建表并指定列簇
    public void createTable(String tableName, String cols[]) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))){
            System.out.println("表【" + tableName + "】存在");
        } else {
            HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(tableName));
            for (String s : cols) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(s);
                hbaseTable.addFamily(columnDescriptor);
            }
            admin.createTable(hbaseTable);
            admin.close();
        }
    }

    //删除表
    public void deleteTable(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }
    }

    //添加数据
    /**
     * @param tableName 表名
     * @param rowkey 行号
     * @param colFamily 列簇
     * @param column 列
     * @param value 列值
     */
    public void insertData(String tableName, String rowkey, String colFamily, String column, String value) throws IOException {
        Table table = (Table) connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    //查询一条数据
    public String getCellByRowkey(String tableName, String rowkey, String columnFamily, String column) throws IOException {
        String res = null;
        Table table = (Table) connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));

        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        Result result = table.get(get);
        res = new String(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column)));
        table.close();
        return res;
    }

    //
    // 查询一条数据
    // 参考url：https://blog.csdn.net/weixin_39815435/article/details/114448801
    // java hbase get_Java 操作 HBase 教程
    //
    public Map getOneByRowkey(String tableName, String rowkey) throws IOException {
        String res = null;
        Table table = (Table) connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);

        List<Cell> cells = result.listCells();
        if (CollectionUtils.isEmpty(cells)) {
            return Collections.emptyMap();

        }
        Map objectMap = new HashMap<>();
        for (Cell cell : cells) {
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell), "UTF-8");
            objectMap.put(qualifier, value);
        }
        table.close();
        return objectMap;
    }

    //删除一条数据
    @SuppressWarnings("empty-statement")
    public void deleteByRow(String tableName, String rowkey) throws IOException {
        Table table = (Table) connection.getTable(TableName.valueOf(tableName));

        //删除一条数据
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
        //删除多条数据
        String[] rowkeys = {};
        List<Delete> list = new java.util.ArrayList<Delete>();
        for (String rk : rowkeys) {
            Delete d = new Delete(Bytes.toBytes(rk));
            list.add(d);
        }
        table.delete(list);
        table.close();
    }

    public static void main(String[] args) throws ZooKeeperConnectionException, IOException {
        String tableName = "mytest";
        String cols[] = {"a", "b", "c"};

        HbaseTest hbaseTest = new HbaseTest();
        hbaseTest.init();
        hbaseTest.getImage(tableName, "1");
    }
}