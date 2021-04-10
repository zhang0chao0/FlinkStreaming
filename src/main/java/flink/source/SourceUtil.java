package flink.source;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.MysqlUtil;

import java.util.HashMap;

import static flink.sqlbuild.SqlBuild.EVENT_TIME;
import static org.apache.flink.table.api.Expressions.$;

public class SourceUtil {


    private static final Logger LOGGER = LoggerFactory.getLogger("business");

    // 支持的数据类型
    public static final HashMap<String, TypeInformation> typeHashMap = new HashMap();
    // 避免和系统 INT 冲突，加上 MY_
    public static final String MY_INT = "INT";
    public static final String MY_STRING = "STRING";
    public static final String MY_TIMESTAMP = "TIMESTAMP";
    public static final String MY_LONG = "LONG";
    public static final String MY_FLOAT = "FLOAT";
    public static final String MY_DOUBLE = "DOUBLE";
    public static final String MY_BIGINT = "BIGINT";
    public static final String FIELD_NAME = "field_name";
    public static final String FIELD_TYPE = "field_type";

    static {
        typeHashMap.put(MY_INT, Types.INT);
        typeHashMap.put(MY_STRING, Types.STRING);
        typeHashMap.put(MY_TIMESTAMP, Types.SQL_TIMESTAMP);
        typeHashMap.put(MY_LONG, Types.LONG);
        typeHashMap.put(MY_FLOAT, Types.FLOAT);
        typeHashMap.put(MY_DOUBLE, Types.DOUBLE);
        typeHashMap.put(MY_BIGINT, Types.LONG);
    }

    // 实时数据源存储
    public static final String KAFKA = "kafka";
    public static final String TOPIC = "topic";
    // 静态数据源存储
    public static final String MYSQL = "mysql";
    public static final String MY_TABLE = "table";

    public static final String STREAM = "stream";
    public static final String STATIC = "static";

    /*
    根据表ID，获取 RowTypeInfo 数组供 row 使用，String -> Row 需要
     */
    public static RowTypeInfo getRowTypeInfo(String inputTable) {
        MysqlUtil meta = new MysqlUtil();
        JSONArray schema = JSONArray.parseArray(meta.getMetaColumnById(inputTable, MysqlUtil.MyMeta.schema));
        meta.close();
        TypeInformation[] fieldTypes = new TypeInformation[schema.size()];
        int index = 0;
        // 根据 schema 确定
        for (Object key : schema) {
            JSONObject jKey = (JSONObject) key;
            String type = jKey.getString(FIELD_TYPE);
            fieldTypes[index++] = typeHashMap.get(type);
        }
        return new RowTypeInfo(fieldTypes);
    }

    /*
    根据表ID，获取 Expression[] 数组，Row -> Table 需要
     */
    public static Expression[] getExpressionArray(String inputTable) {
        MysqlUtil meta = new MysqlUtil();
        JSONArray schema = JSONArray.parseArray(meta.getMetaColumnById(inputTable, MysqlUtil.MyMeta.schema));
        meta.close();
        Expression[] expressions = new Expression[schema.size()];
        // 第一个字段是时间字段 eventTime，约定
        expressions[0] = $(EVENT_TIME).rowtime();
        for (int i = 1; i < schema.size(); i++) {
            JSONObject jKey = (JSONObject) schema.get(i);
            String name = jKey.getString(FIELD_NAME);
            expressions[i] = $(name);
        }
        return expressions;
    }

    /*
    根据表ID，添加数据源
     */
    public static void addSource(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String inputTable) {
        MysqlUtil meta = new MysqlUtil();
        String tableType = meta.getMetaColumnById(inputTable, MysqlUtil.MyMeta.table_type);
        String storages = meta.getMetaColumnById(inputTable, MysqlUtil.MyMeta.storages);
        meta.close();
        if (tableType.equals(STREAM)) {
            // 实时数据源
            JSONObject jsonObject = (JSONObject) JSONObject.parse(storages);
            String topic = jsonObject.getJSONObject(KAFKA).getString(TOPIC);
            KafkaSource.addSource(env, tEnv, inputTable, topic);
        } else {
            // 静态数据源
            JSONObject jsonObject = (JSONObject) JSONObject.parse(storages);
            String table = jsonObject.getJSONObject(MYSQL).getString(MY_TABLE);
            StaticSource.addSource(inputTable, table);
        }
    }
}
