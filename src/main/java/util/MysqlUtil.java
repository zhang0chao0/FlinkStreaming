package util;

import com.alibaba.fastjson.JSONArray;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


// 简单的 JDBC 连接工具
public class MysqlUtil {

    public static final String DRIVER = "com.mysql.jdbc.Driver";
    public static final String URL = "jdbc:mysql://localhost:3306/flinkLearn?useUnicode=true&characterEncoding=UTF-8";
    public static final String USER = "root";
    public static final String PASSWD = "admin";

    // 字段名
    public enum MyMeta {
        table_type, schema, storages;
    }

    public enum MyTableSql {
        select_type, select_sql;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger("business");
    private Connection connection;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet;

    public MysqlUtil() {
        try {
            Class.forName(DRIVER);
            connection = DriverManager.getConnection(URL, USER, PASSWD);
        } catch (Exception e) {
            LOGGER.error("连接MySQL异常：" + e.getMessage());
        }
    }


    public String getMetaColumnById(String tableId, MyMeta column) {
        String result = null;
        try {
            String sql = null;
            if (column == MyMeta.table_type) {
                sql = "select `table_type` from `table_meta` where `table_id` = ?";
            } else if (column == MyMeta.schema) {
                sql = "select `schema` from `table_meta` where `table_id` = ?";
            } else if (column == MyMeta.storages) {
                sql = "select `storages` from `table_meta` where `table_id` = ?";
            }
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, tableId);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                result = resultSet.getString(1);
            }
        } catch (SQLException ex) {
            LOGGER.error("查询column异常，tableId=" + tableId + "，column=" + column + "，报错：" + ex.getMessage());
        }
        return result;
    }

    public void insertOrUpdateTableMeta(String tableId, String tableType, String schema, String storages) {
        try {
            String sql = null;
            if (getMetaColumnById(tableId, MyMeta.table_type) == null) {
                // 没有数据则插入
                sql = "insert into `table_meta` values (?,?,?,?)";
                preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setString(1, tableId);
                preparedStatement.setString(2, tableType);
                preparedStatement.setString(3, schema);
                preparedStatement.setString(4, storages);
            } else {
                // 有则更新
                sql = "update `table_meta` set `table_type` = ?, `schema` = ?, `storages` = ? where `table_id` = ?";
                preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setString(1, tableType);
                preparedStatement.setString(2, schema);
                preparedStatement.setString(3, storages);
                preparedStatement.setString(4, tableId);
            }
            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            LOGGER.error("插入或更新 table_meta 表异常：" + ex.getMessage());
        }
    }

    public String getTableSqlById(String tableId, MyTableSql column) {
        String result = null;
        try {
            String sql = null;
            if (column == MyTableSql.select_type) {
                sql = "select `select_type` from `table_sql` where `table_id` = ?";
            } else if (column == MyTableSql.select_sql) {
                sql = "select `select_sql` from `table_sql` where `table_id` = ?";
            }
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, tableId);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                result = resultSet.getString(1);
            }
        } catch (SQLException ex) {
            LOGGER.error("查询column异常，tableId=" + tableId + "，column=" + column + "，报错：" + ex.getMessage());
        }
        return result;
    }

    public void insertOrUpdateTableSql(String tableId, String selectType, String selectSql) {
        try {
            String sql = null;
            if (getTableSqlById(tableId, MyTableSql.select_type) == null) {
                sql = "insert into `table_sql` values (?,?,?)";
                preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setString(1, tableId);
                preparedStatement.setString(2, selectType);
                preparedStatement.setString(3, selectSql);
            } else {
                sql = "update `table_sql` set select_type = ?, select_sql = ? where table_id = ?";
                preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setString(1, selectType);
                preparedStatement.setString(2, selectSql);
                preparedStatement.setString(3, tableId);
            }
            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            LOGGER.error("插入或更新 table_sql 表异常：" + ex.getMessage());
        }
    }


    public List<Row> getStaticData(String inputTable, String tableName) {
        List<Row> staticList = new ArrayList<>();
        JSONArray meta = JSONArray.parseArray(this.getMetaColumnById(inputTable, MyMeta.schema));
        int rowSize = meta.size();
        String sql = "select * from " + tableName;
        try {
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                Row row = new Row(rowSize);
                for (int i = 0; i < rowSize; i++) {
                    row.setField(i, resultSet.getObject(i + 1));
                }
                staticList.add(row);
            }
        } catch (SQLException ex) {
            LOGGER.error("查询静态数据异常，报错：" + ex.getMessage());
        }
        return staticList;
    }

    public void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ex) {
            LOGGER.error("关闭数据库连接异常：" + ex.getMessage());
        }
    }

}
