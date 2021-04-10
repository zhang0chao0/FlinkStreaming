package flink.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.MysqlUtil;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import static flink.source.SourceUtil.FIELD_NAME;
import static flink.sqlbuild.SqlBuild.EVENT_TIME;
import static flink.sqlbuild.SqlBuild.TIME_PATTERN;


/*
输出的时候使用
Row -> String，输出到 kafka
 */
public class Row2StrFunction extends RichMapFunction<Row, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger("business");

    private final String tableId;
    private JSONArray schema;
    private MysqlUtil meta;

    public Row2StrFunction(String tableId) {
        this.tableId = tableId;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        meta = new MysqlUtil();
        schema = JSONArray.parseArray(meta.getMetaColumnById(tableId, MysqlUtil.MyMeta.schema));
    }

    @Override
    public void close() throws Exception {
        meta.close();
    }

    @Override
    public String map(Row value) throws Exception {
        // json 数据
        JSONObject result = new JSONObject();
        // 遍历 schema
        for (int i = 0; i < schema.size(); i++) {
            JSONObject jKey = schema.getJSONObject(i);
            String field_name = jKey.getString(FIELD_NAME);
            Object field_value = value.getField(i);
            /*
            把时间字段加 8 小时
            sql 计算出来的类型是 LocalDateTime
            stream join 出来的是 Timestamp
             */
            if (field_name.equals(EVENT_TIME)) {
                if (field_value instanceof LocalDateTime) {
                    // 取出 time，+ 8 小时
                    String timeStr = ((LocalDateTime) field_value)
                            .plusHours(8)
                            .format(DateTimeFormatter.ofPattern(TIME_PATTERN));
                    result.put(field_name, timeStr);
                } else if (field_value instanceof Timestamp) {
                    String timeStr = new SimpleDateFormat(TIME_PATTERN)
                            .format(new Date(((Timestamp) field_value).getTime()));
                    result.put(field_name, timeStr);
                } else {
                    assert field_value != null;
                    LOGGER.error("event_time字段为不支持的时间格式=" + field_value.getClass());
                }
            } else {
                result.put(field_name, field_value);
            }
        }
        return result.toString();
    }
}
