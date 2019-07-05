package com.kgc;

import com.kgc.util.HBaseConfigUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseApi {
    Connection connection = null;
    Admin admin = null;
    Table table = null;
    ResultScanner resultScanner = null;
    private HBaseConfigUtil hBaseConfigUtil = new HBaseConfigUtil();

    // 创建表
    public boolean createTable(String tablename, String[] familyNames) {
        boolean sign = false;
        try {
            this.connection = ConnectionFactory.createConnection(HBaseConfigUtil.getHbaseConfiguration());
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(tablename);
            if (!admin.isTableAvailable(tableName)) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                for (String familyName : familyNames) {
                    hTableDescriptor.addFamily(new HColumnDescriptor(familyName));
                }
                admin.createTable(hTableDescriptor);
            }
            sign = true;
        } catch (IOException e) {
            e.printStackTrace();

        } finally {
            clossAll();
            return sign;
        }
    }

    public boolean deltable(String tablename) {
        boolean sign = false;
        try {
            this.connection = ConnectionFactory.createConnection(HBaseConfigUtil.getHbaseConfiguration());
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(tablename);
            if (admin.isTableAvailable(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                sign = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            clossAll();
            return sign;
        }
    }

    //查询库表列表
    public HTableDescriptor[] listTables() {
        HTableDescriptor[] hTableDescriptors = null;
        try {
            connection = ConnectionFactory.createConnection(HBaseConfigUtil.getHbaseConfiguration());
            admin = connection.getAdmin();
            hTableDescriptors = admin.listTables();
            //获取内部信息 HTableDescriptor hTableDescriptor =>hTableDescriptor.getTableName();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            clossAll();
            return hTableDescriptors;
        }
    }

    //插入数据
    //示例数据         rowkey      frist     last     email         sex  age
    // {              {"1", "Marcel", "Haddad", "marcel@xyz.com", "M", "26"},
    //                {"2", "Franklin", "Holtz", "franklin@xyz.com", "M", "24"},
    //                {"3", "Dwayne", "McKee", "dwayne@xyz.com", "M", "27"},
    //                {"4", "Rae", "Schroeder", "rae@xyz.com", "F", "31"},
    //                {"5", "Rosalie", "burton", "rosalie@xyz.com", "F", "25"},
    //                {"6", "Gabriela", "Ingram", "gabriela@xyz.com", "F", "24"}         }
    public boolean insertResource(String tablename, String[][] words) {
        boolean sign = false;
        try {
            connection = ConnectionFactory.createConnection(HBaseConfigUtil.getHbaseConfiguration());
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(tablename);
            if (admin.isTableAvailable(tableName)) {
                table = connection.getTable(tableName);
                for (int i = 0; i < words.length; i++) {
                    Put put = new Put(Bytes.toBytes(words[i][0]));
                    put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("frist"), Bytes.toBytes(words[i][1]));
                    put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("last"), Bytes.toBytes(words[i][2]));
                    put.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"), Bytes.toBytes(words[i][3]));
                    put.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("sex"), Bytes.toBytes(words[i][4]));
                    put.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"), Bytes.toBytes(words[i][5]));
                    table.put(put);
                    sign = true;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            clossAll();
            return sign;
        }
    }

    //获取指定key 的值
    //key 事例为 {"3","name","last"} 分别代表 {rowKey , 主列 , 从列}
    public String getValueofKey(String tablename, String[] key) {
        Result result = null;
        String answer = null;
        try {
            this.connection = ConnectionFactory.createConnection(HBaseConfigUtil.getHbaseConfiguration());
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(tablename);
            if (admin.isTableAvailable(tableName)) {
                table = connection.getTable(tableName);
                Get get = new Get(Bytes.toBytes(key[0]));
                result = table.get(get);
                byte[] lastName = result.getValue(Bytes.toBytes(key[1]), Bytes.toBytes(key[2]));
                return answer = Bytes.toString(lastName).toString();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            clossAll();
            return answer;
        }
    }

    //删除数据
    //根据rowkey 删除
    //示例数据
    // {
    //                {"1", "Marcel", "Haddad", "marcel@xyz.com", "M", "26"},
    //                {"2", "Franklin", "Holtz", "franklin@xyz.com", "M", "24"},
    //                {"3", "Dwayne", "McKee", "dwayne@xyz.com", "M", "27"},
    //                {"4", "Rae", "Schroeder", "rae@xyz.com", "F", "31"},
    //                {"5", "Rosalie", "burton", "rosalie@xyz.com", "F", "25"},
    //                {"6", "Gabriela", "Ingram", "gabriela@xyz.com", "F", "24"}         }
    public boolean delResource(String tablename, String[][] words) {
        boolean sign = false;
        try {
            connection = ConnectionFactory.createConnection(HBaseConfigUtil.getHbaseConfiguration());
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(tablename);
            if (admin.isTableAvailable(tableName)) {
                table = connection.getTable(tableName);
                List<Delete> list = new ArrayList<>();
                for (int i = 0; i < 2; i++) {
                    Delete delete = new Delete(Bytes.toBytes(words[i][0]));
                    list.add(delete);
                }
                table.delete(list);
                sign = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            clossAll();
            return sign;
        }
    }

    //筛选表数据
    // 示例数据为
    // {   {"name","frist","EQUAL","zhangsan"},
    //     {"personalinfo","sex","NOT_EQUAL","F"},
    //     {"personalinfo","age","GREATER","25"}}
    public void scanData(String tablename, String[][] filters) {
        try {
            connection = ConnectionFactory.createConnection(HBaseConfigUtil.getHbaseConfiguration());
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(tablename);
            if (admin.isTableAvailable(tableName)) {
                table = connection.getTable(tableName);
                Scan scan = new Scan();
                FilterList filterList = new FilterList();
                for (String[] strings : filters) {
                    CompareFilter.CompareOp condition = null;
                    switch (strings[2]) {
                        case "LESS":
                            condition = CompareFilter.CompareOp.LESS;
                            break;
                        case "LESS_OR_EQUAL":
                            condition = CompareFilter.CompareOp.LESS_OR_EQUAL;
                            break;
                        case "EQUAL":
                            condition = CompareFilter.CompareOp.EQUAL;
                            break;
                        case "NOT_EQUAL":
                            condition = CompareFilter.CompareOp.NOT_EQUAL;
                            break;
                        case "GREATER_OR_EQUAL":
                            condition = CompareFilter.CompareOp.GREATER_OR_EQUAL;
                            break;
                        case "GREATER":
                            condition = CompareFilter.CompareOp.GREATER;
                            break;
                        default:
                            condition = CompareFilter.CompareOp.GREATER;
                    }
                    SingleColumnValueExcludeFilter singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(
                            Bytes.toBytes(strings[0]), Bytes.toBytes(strings[1]), condition, Bytes.toBytes(strings[3]));
                    filterList.addFilter(singleColumnValueExcludeFilter);
                }
                scan.setFilter(filterList);
                resultScanner = table.getScanner(scan);
                Result result = resultScanner.next();
                while (result != null) {
                    byte[] firstname = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("frist"));
                    byte[] lastname = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("last"));
                    byte[] contactinfo_email = result.getValue(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"));
                    byte[] personalinfo_sex = result.getValue(Bytes.toBytes("personalinfo"), Bytes.toBytes("sex"));
                    byte[] personalinfo_age = result.getValue(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"));
                    System.out.println("姓名:" + Bytes.toString(firstname) + " " + Bytes.toString(lastname)
                            + "  邮箱为:" + Bytes.toString(contactinfo_email) + "  性别: " + Bytes.toString(personalinfo_sex) +
                            " 年龄:" + Bytes.toString(personalinfo_age));
                    result = resultScanner.next();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            clossAll();
        }
    }
    //关连接
    public void clossAll() {
        if (admin != null) {
            try {
                admin.close();
                admin = null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
                connection = null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (table != null) {
            try {
                table.close();
                table = null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (resultScanner != null) {
            resultScanner.close();
            resultScanner = null;
        }
    }
}
