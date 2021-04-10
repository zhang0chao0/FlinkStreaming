package flink.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;
import util.MysqlUtil;

import static flink.source.SourceUtil.FIELD_NAME;
import static flink.source.SourceUtil.FIELD_TYPE;
import static flink.source.SourceUtil.MY_DOUBLE;
import static flink.source.SourceUtil.MY_FLOAT;
import static flink.source.SourceUtil.MY_INT;
import static flink.source.SourceUtil.MY_LONG;
import static flink.source.SourceUtil.MY_TIMESTAMP;
import static flink.source.SourceUtil.MY_BIGINT;


/*
ObjectNode 转化为 Row
ObjectNode 相比 String 可以获取 kafka offset 等信息
 */
public class Str2RowFunction extends RichMapFunction<ObjectNode, Row> {

    private final String tableId;
    private JSONArray schema;
    private MysqlUtil meta;

    public Str2RowFunction(String tableId) {
        this.tableId = tableId;
    }

    @Override
    public void open(Configuration parameters) {
        meta = new MysqlUtil();
        schema = JSONArray.parseArray(meta.getMetaColumnById(tableId, MysqlUtil.MyMeta.schema));
    }

    @Override
    public void close() {
        meta.close();
    }

    @Override
    public Row map(ObjectNode value) throws Exception {
        // json 数据
        JSONObject jsonObject = (JSONObject) JSONObject.parse(value.get("value").toString());
        Row row = new Row(schema.size());
        // 根据 schema 确定 row
        int i = 0;
        for (Object key : schema) {
            JSONObject jKey = (JSONObject) key;
            String field_name = jKey.getString(FIELD_NAME);
            String field_type = jKey.getString(FIELD_TYPE);
            switch (field_type) {
                case MY_TIMESTAMP:
                    row.setField(i++, jsonObject.getTimestamp(field_name));
                    break;
                case MY_INT:
                    row.setField(i++, jsonObject.getIntValue(field_name));
                    break;
                case MY_LONG:
                case MY_BIGINT:
                    row.setField(i++, jsonObject.getLongValue(field_name));
                    break;
                case MY_FLOAT:
                    row.setField(i++, jsonObject.getFloatValue(field_name));
                    break;
                case MY_DOUBLE:
                    row.setField(i++, jsonObject.getDoubleValue(field_name));
                    break;
                // String
                default:
                    row.setField(i++, jsonObject.getString(field_name));
                    break;
            }
        }
        return row;
    }
}
