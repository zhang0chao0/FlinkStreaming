package flink.sqlbuild;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import dataflow.ProcessNode;
import flink.function.Str2RowFunction;
import flink.source.SourceUtil;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import util.MysqlUtil;
import util.ProjectConfigReader;
import util.SqlUtil;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static flink.source.SourceUtil.FIELD_NAME;
import static flink.source.SourceUtil.FIELD_TYPE;
import static flink.source.SourceUtil.KAFKA;
import static flink.source.SourceUtil.MY_BIGINT;
import static flink.source.SourceUtil.MY_STRING;
import static flink.source.SourceUtil.MY_TIMESTAMP;
import static flink.source.SourceUtil.STREAM;
import static flink.source.SourceUtil.TOPIC;
import static flink.sqlbuild.SqlBuild.EVENT_TIME;
import static flink.sqlbuild.SqlBuild.WINDOW_END;
import static flink.sqlbuild.SqlBuild.WINDOW_START;

/*
普通 sql 查询
inner join、left join、right join
解析后保存元数据
 */
public class ConfigUtil {

    public static final String ROW_TIME_TYPE = "*ROWTIME*";

    public static String delTag(String input) {
        // 删除字符 `
        if (input.length() == 0) {
            return "";
        }
        char[] chars = input.toCharArray();
        int i = 0;
        int j = chars.length - 1;
        if (chars[i] == '`') {
            i++;
        }
        if (chars[j] == '`') {
            j--;
        }
        StringBuilder sb = new StringBuilder();
        for (; i <= j; i++) {
            sb.append(chars[i]);
        }
        return sb.toString();
    }

    public static String buildJoinConfig(String outputTableId, SqlUtil sqlUtil) {
        JoinConfig joinConfig = new JoinConfig();
        String tableName = sqlUtil.getFromTable().getName();
        // 去除头尾空格
        String tableAlias = sqlUtil.getFromTable().getAlias().toString().trim();
        joinConfig.setTableName(tableName);
        Join join = sqlUtil.getJoins().get(0);
        Table joinTable = (Table) join.getRightItem();
        String joinTableName = joinTable.getName();
        String joinAlias = joinTable.getAlias().toString().trim();
        joinConfig.setJoinTableName(joinTableName);
        // a.id = b.id
        EqualsTo equalsTo = (EqualsTo) join.getOnExpression();
        Column left = (Column) equalsTo.getLeftExpression();
        String leftTableName = left.getTable().getName();
        // 主表的查询条件 a.id
        if (leftTableName.equals(tableName) || leftTableName.equals(tableAlias)) {
            joinConfig.setOnTableField(left.getColumnName());
        }
        Column right = (Column) equalsTo.getRightExpression();
        String rightTableName = right.getTable().getName();
        if (rightTableName.equals(joinTableName) || rightTableName.equals(joinAlias)) {
            joinConfig.setOnJoinField(right.getColumnName());
        }
        List<String> selectTableList = new ArrayList<>();
        List<String> joinTableList = new ArrayList<>();
        for (SelectItem item : sqlUtil.getSelectItems()) {
            SelectExpressionItem sItem = (SelectExpressionItem) item;
            Column column = (Column) sItem.getExpression();
            String columnTableName = column.getTable().getName();
            if (columnTableName.equals(tableName) || columnTableName.equals(tableAlias)) {
                selectTableList.add(delTag(column.getColumnName()));
            } else {
                joinTableList.add(delTag(column.getColumnName()));
            }
        }
        joinConfig.setSelectTableField(selectTableList);
        joinConfig.setSelectJoinField(joinTableList);
        // 存元数据
        addJoinMeta(outputTableId, joinConfig);
        return JSONObject.toJSONString(joinConfig);
    }

    public static void addJoinMeta(String outputTableId, JoinConfig joinConfig) {
        // 添加元数据
        String table1 = joinConfig.getTableName();
        String table2 = joinConfig.getJoinTableName();
        // 确保被 join 的表，元数据一定提前就有
        // SQL Parse 一定是单独抽一个模块出来！
        MysqlUtil meta = new MysqlUtil();
        JSONArray t1Meta = JSONArray.parseArray(meta.getMetaColumnById(table1, MysqlUtil.MyMeta.schema));
        HashMap<String, String> h1 = new HashMap<>();
        for (Object obj : t1Meta) {
            JSONObject jsonObject = (JSONObject) obj;
            h1.put(jsonObject.getString(FIELD_NAME), jsonObject.getString(FIELD_TYPE));
        }
        JSONArray t2Meta = JSONArray.parseArray(meta.getMetaColumnById(table2, MysqlUtil.MyMeta.schema));
        HashMap<String, String> h2 = new HashMap<>();
        for (Object obj : t2Meta) {
            JSONObject jsonObject = (JSONObject) obj;
            h2.put(jsonObject.getString(FIELD_NAME), jsonObject.getString(FIELD_TYPE));
        }
        // 新表的 schema 继承于 输入表，同时加上三个固定的字段
        // EVENT_TIME、WINDOW_START、WINDOW_END
        JSONArray newSchema = new JSONArray();
        JSONObject j1 = new JSONObject();
        j1.put(FIELD_NAME, EVENT_TIME);
        j1.put(FIELD_TYPE, MY_TIMESTAMP);
        JSONObject j2 = new JSONObject();
        j2.put(FIELD_NAME, WINDOW_START);
        j2.put(FIELD_TYPE, MY_STRING);
        JSONObject j3 = new JSONObject();
        j3.put(FIELD_NAME, WINDOW_END);
        j3.put(FIELD_TYPE, MY_STRING);
        newSchema.add(j1);
        newSchema.add(j2);
        newSchema.add(j3);
        // 同时加上新 select 的字段
        buildSchemaMap(newSchema, h1, joinConfig.getSelectTableField());
        buildSchemaMap(newSchema, h2, joinConfig.getSelectJoinField());
        // 默认给它一个存储
        JSONObject topicObj = new JSONObject();
        topicObj.put(TOPIC, outputTableId);
        JSONObject storageObj = new JSONObject();
        storageObj.put(KAFKA, topicObj);
        String storages = storageObj.toString();
        meta.insertOrUpdateTableMeta(
                outputTableId,
                STREAM,
                newSchema.toString(),
                storages
        );
        meta.close();
    }

    public static void buildSchemaMap(JSONArray schema, HashMap<String, String> schemaMap, List<String> fieldList) {
        // 根据输入表，确定输出表的 schema
        for (String item : fieldList) {
            JSONObject jt = new JSONObject();
            // 无别名直接映射
            jt.put(FIELD_NAME, item);
            jt.put(FIELD_TYPE, schemaMap.get(item));
            schema.add(jt);
        }
    }

    public static String getSchemaString(TableSchema tableSchema) {
        // TableColumn 第一个字段一定是 EVENT_TIME
        JSONArray saveSchema = new JSONArray();
        for (TableColumn tableColumn : tableSchema.getTableColumns()) {
            JSONObject temp = new JSONObject();
            // TIMESTAMP(3) *ROWTIME* 类型转化为 TIMESTAMP
            if (tableColumn.getType().toString().contains(ROW_TIME_TYPE)) {
                temp.put(FIELD_TYPE, MY_TIMESTAMP);
            } else if (tableColumn.getType().toString().contains(MY_BIGINT)) {
                temp.put(FIELD_TYPE, MY_BIGINT);
            } else {
                temp.put(FIELD_TYPE, tableColumn.getType().toString());
            }
            temp.put(FIELD_NAME, tableColumn.getName());
            saveSchema.add(temp);
        }
        return saveSchema.toString();
    }

    public static void buildSqlSchema(StreamExecutionEnvironment env,
                                      StreamTableEnvironment tEnv,
                                      ProcessNode processNode,
                                      String flinkSql) {
        // 对于单表，保存 schema 和存储信息
        String inputTableId = processNode.getInputTableList().get(0);
        String outputTableId = processNode.getOutputTable();
        DataStream<Row> sourceRow = env.addSource(
                new FlinkKafkaConsumer<>(
                        "",
                        new JSONKeyValueDeserializationSchema(true),
                        ProjectConfigReader.kafkaProperties(ProjectConfigReader.kafka_prop.kafka_consumer)
                )
        )
                .map(new Str2RowFunction(inputTableId))
                .returns(SourceUtil.getRowTypeInfo(inputTableId))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Row>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withIdleness(Duration.ofMinutes(2))
                        .withTimestampAssigner((r, t) -> {
                            // 约定第一个是时间字段，类型为 Timestamp
                            // 这里只是获取时间戳
                            Timestamp curT = (Timestamp) r.getField(0);
                            assert curT != null;
                            return curT.getTime();
                        }));
        tEnv.createTemporaryView(inputTableId, sourceRow, SourceUtil.getExpressionArray(inputTableId));
        String schemaString = getSchemaString(tEnv.sqlQuery(flinkSql).getSchema());
        JSONObject topicObj = new JSONObject();
        topicObj.put(TOPIC, outputTableId);
        JSONObject storageObj = new JSONObject();
        storageObj.put(KAFKA, topicObj);
        String storages = storageObj.toString();
        new MysqlUtil().insertOrUpdateTableMeta(
                outputTableId,
                STREAM,
                schemaString,
                storages
        );
    }
}
