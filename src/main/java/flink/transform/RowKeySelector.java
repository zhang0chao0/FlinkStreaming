package flink.transform;

import com.alibaba.fastjson.JSONArray;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.types.Row;
import util.MysqlUtil;


/*
Row join 时，取出 key
 */
public class RowKeySelector implements KeySelector<Row, String> {

    private final String keyByValue;
    private final JSONArray schema;

    public RowKeySelector(String tableId, String keyByValue) {
        this.keyByValue = keyByValue;
        schema = JSONArray.parseArray(new MysqlUtil().getMetaColumnById(tableId, MysqlUtil.MyMeta.schema));
    }

    @Override
    public String getKey(Row value) throws Exception {
        return JoinUtil.getKey(keyByValue, value, schema);
    }
}
