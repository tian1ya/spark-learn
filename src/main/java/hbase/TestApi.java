//package hbase;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.*;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//import scala.util.control.Exception;
//import sun.tools.jconsole.Tab;
//
//import java.io.IOException;
//
///*
//    DDL
//    1. 创建命名空间
//    2. 表的增删
//    3. 删除表
//
//    DML
//    5. 插入数据
//    6. 查数据
//        get
//        scan
//
//    7. 删除数据
// */
//public class TestApi {
//
//    private static Connection connection = null;
//    private static Admin admin = null;
//
//    static {
//        try {
//            Configuration conf = HBaseConfiguration.create();
//            conf.set("hbase.zookeeper.quorum","node-1,node-2,node-3");
//            connection = ConnectionFactory.createConnection(conf);
//            admin = connection.getAdmin();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    /*
//        判断表是否存在
//     */
//
//    public static boolean isTableExist(String tableName) throws IOException {
//
//        //1. 获取配置文件信息
////        HBaseConfiguration conf = new HBaseConfiguration();
//        // 通过zk 连接到hbase， hbase.zookeeper.quorum 是 hbase-site.xml 中的配置文件
//        // 表示的是zk 的安装机器ip，这里不需要写port，是因为zk 默认的是2181 这个端口
////        conf.set("hbase.zookeeper.quorum","node-1,node-2,node-3");
//
//        /*
//            1.1 上面的连接方法使用的是已经作废了的方法，
//            下面使用新的方法
//         */
//
////        Configuration conf = HBaseConfiguration.create();
////        conf.set("hbase.zookeeper.quorum","node-1,node-2,node-3");
//        /*
//            获取管理员对象
//            这里的这个方法也是过期的了
//         */
////        HBaseAdmin admin = new HBaseAdmin(conf);
//
////        Connection connection = ConnectionFactory.createConnection(conf);
////        Admin admin = connection.getAdmin();
//
//        /*
//            表是否存在
//         */
//        boolean exist = admin.tableExists(TableName.valueOf(tableName));
//
////        admin.close();
//        return exist;
//    }
//
//
//    public static void close() {
//        if (admin != null) {
//            try {
//                admin.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        if (connection != null) {
//            try {
//                connection.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public static void createTable(String  tableName, String... cfs) throws IOException {
//        if (cfs.length <= 0) {
//            System.out.println("列族不能为空");
//            return;
//        }
//
//        if (isTableExist(tableName)) {
//            System.out.println("table" + tableName + "已存在");
//            return;
//        }
//
//        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
//        for (String cf : cfs) {
//            HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
////            columnDescriptor.setMaxVersions()
//            tableDescriptor.addFamily(columnDescriptor);
//        }
//        admin.createTable(tableDescriptor);
//    }
//
//    public static void delete(String tableName) throws IOException {
//        if (!isTableExist(tableName)) {
//            System.out.println(tableName + "不存在");
//            return;
//        }
//
//        TableName hTableName = TableName.valueOf(tableName);
//        admin.disableTable(hTableName);
//        admin.deleteTable(hTableName);
//    }
//
//    public static void createNameSpace(String ns) {
//        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
//        try {
//            admin.createNamespace(namespaceDescriptor);
//        }catch (NamespaceExistException e) {
//            System.out.println(ns + "命名空间已存在");
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public static void putData(String tableName, String rowkey, String cf, String c, String value) throws IOException {
//        Table table = null;
//        try {
//            // table 是轻量级的连接，并不需要放到连接池中，随时创建，用完销毁OK
//            table = connection.getTable(TableName.valueOf(tableName));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        /*
//            Bytes 工具类将 int double 等专为 bytes， 并将 bytes 转回到 int double
//         */
//        Put put = new Put(Bytes.toBytes(rowkey));
//
//        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(c), Bytes.toBytes(value));
//
//        // 给同一个rowkey 添加多个列
////        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("sex"), Bytes.toBytes("male"));
//
//        // 插入多个 rowkey
//        // 这个时候需要构建多个rowkey 的数据，list<Put>
//        table.put(put);
//        table.close();
//    }
//
//    public static void getData(String tableName, String rowkey,String cf,String cn) throws IOException {
//        Table table = null;
//        try {
//            // table 是轻量级的连接，并不需要放到连接池中，随时创建，用完销毁OK
//            table = connection.getTable(TableName.valueOf(tableName));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        // 获取有一条数据
//        Get get = new Get(Bytes.toBytes(rowkey));
//
//        // 指定获取列族
////        get.addFamily(Bytes.toBytes(cf));
//
//        // 指定列, 列不能脱离列族而存在
//        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
//
//        // 设置获取数据的最大版本数
//        get.setMaxVersions(5);
//        Result result = table.get(get);
//
//
//        //解析 result 并打印
//        Cell[] cells = result.rawCells();
//        for (Cell cell : cells) {
//            // 打印数据
//            String cdf = Bytes.toString(CellUtil.cloneFamily(cell));
//            String clumn = Bytes.toString(CellUtil.cloneQualifier(cell));
//            String value = Bytes.toString(CellUtil.cloneValue(cell));
//            System.out.println("cf:" + cdf + " : "+"clumn:" + clumn + " : "+ "value:" + value);
//        }
//    }
//
//    public static void scanTable(String tableName) throws IOException {
//        Table table = connection.getTable(TableName.valueOf(tableName));
//
////        Scan scan = new Scan();
//        Scan scan = new Scan(Bytes.toBytes("00001"), Bytes.toBytes("00002"));
//        ResultScanner scanner = table.getScanner(scan);
//
//        for (Result result : scanner) {
//            Cell[] cells = result.rawCells();
//            for (Cell cell : cells) {
//                String rk = Bytes.toString(CellUtil.cloneRow(cell));
//                String cdf = Bytes.toString(CellUtil.cloneFamily(cell));
//                String clumn = Bytes.toString(CellUtil.cloneQualifier(cell));
//                String value = Bytes.toString(CellUtil.cloneValue(cell));
//                System.out.println("cf:" + cdf + " : "+"rk: "+rk + ":" +"clumn:" + clumn + " : "+ "value:" + value);
//            }
//        }
//
//        table.close();
//    }
//
//    public static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {
//        /*
//            在命令行里面，会将全部版本的数据给删除掉
//         */
//
//        Table table = connection.getTable(TableName.valueOf(tableName));
//
//        // row 级别的删除, 这个时候回删除 all， 全部版本都删除
////        Delete delete = new Delete(Bytes.toBytes(rowKey));
//
//        Delete delete = new Delete(Bytes.toBytes(rowKey));
//
//        // Delete the latest version of the specified column
//        /*
//            删除最新的数据
//            删除了最新版本的数据，然后旧的数据就出现了，这在生产中会是一个奇怪的事情
//
//         */
////        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
//
//        /*
//            如果是要指定列coulumn 删除所有版本那么需要使用以下的 api
//         */
//        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));
//
//
//        //删除指定小于等于给定时间戳
//
//
//        table.delete(delete);
//
//        table.close();
//    }
//
//    public static void main(String[] args) throws IOException {
////        System.out.println(isTableExist("stu"));
////
////        System.out.println(isTableExist("stu3"));
////        createTable("stu3", "info1","info2");
////        System.out.println(isTableExist("stu3"));
////
////        delete("stu3");
////        System.out.println(isTableExist("stu3"));
//
////        createNameSpace("0408");
//        // 在命名空间下创建一张表
////        createTable("0408:stu3", "info1","info2");
//
////        putData("stu","00002","info1","name","hshs");
////        getData("stu","00002","info1","sex");
//
////        scanTable("stu");
//
//        deleteData("stu", "00003", "info2", "name");
//        close();
//
//    }
//}
