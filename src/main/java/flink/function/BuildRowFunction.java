package flink.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import util.MysqlUtil;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import static flink.source.SourceUtil.FIELD_TYPE;
import static flink.source.SourceUtil.MY_BIGINT;
import static flink.source.SourceUtil.MY_DOUBLE;
import static flink.source.SourceUtil.MY_FLOAT;
import static flink.source.SourceUtil.MY_INT;
import static flink.source.SourceUtil.MY_LONG;
import static flink.source.SourceUtil.MY_TIMESTAMP;

public class BuildRowFunction implements MapFunction<Row, Row> {

    private final JSONArray schema;

    public BuildRowFunction(String tableId) {
        MysqlUtil meta = new MysqlUtil();
        schema = JSONArray.parseArray(meta.getMetaColumnById(tableId, MysqlUtil.MyMeta.schema));
    }

    @Override
    public Row map(Row value) throws Exception {
        Row outRow = new Row(schema.size());
        for (int i = 0; i < schema.size(); i++) {
            JSONObject jsonObject = (JSONObject) schema.get(i);
            String fieldType = jsonObject.getString(FIELD_TYPE);
            Object curValue = value.getField(i);
            assert curValue != null;
            if (fieldType.equals(MY_TIMESTAMP)) {
                // sql 计算出的时间字段是 LocalDateTime
                if (curValue instanceof LocalDateTime) {
                    LocalDateTime localDateTime = (LocalDateTime) curValue;
                    // + 8 小时
                    Timestamp timestamp = Timestamp.valueOf(localDateTime.plusHours(8));
                    outRow.setField(i, timestamp);
                } else {
                    outRow.setField(i, curValue);
                }
            } else if (fieldType.equals(MY_INT)) {
                outRow.setField(i, Integer.parseInt(curValue.toString()));
            } else if (fieldType.equals(MY_BIGINT) || fieldType.equals(MY_LONG)) {
                outRow.setField(i, Long.parseLong(curValue.toString()));
            } else if (fieldType.equals(MY_FLOAT)) {
                outRow.setField(i, Float.parseFloat(curValue.toString()));
            } else if (fieldType.equals(MY_DOUBLE)) {
                outRow.setField(i, Double.parseDouble(curValue.toString()));
            } else {
                // String
                outRow.setField(i, curValue.toString());
            }
        }
        return outRow;
    }
}
