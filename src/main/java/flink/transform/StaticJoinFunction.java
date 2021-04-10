package flink.transform;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import flink.PipeLine;
import flink.sqlbuild.JoinConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.MysqlUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static flink.source.SourceUtil.FIELD_NAME;
import static flink.sqlbuild.SqlBuild.LEFT_JOIN;
import static flink.transform.JoinUtil.NULL_STR;

/*
流与静态表的关联只支持 inner join 和 left join
right join 意义不大
RichFlatMapFunction：inner join 可能没有输出
 */
public class StaticJoinFunction extends RichFlatMapFunction<Row, Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger("business");

    // inner，left
    private final String joinType;
    private final JoinConfig joinConfig;
    private final String inputTable1;
    private final String inputTable2;
    private JSONArray inputTable1Schema;
    private JSONArray inputTable2Schema;
    private final String outputTableId;
    private JSONArray outputTableSchema;
    private MysqlUtil meta;
    // 一行有多少数据
    private int rowLen;
    // 按查询的 key 存放静态数据
    private HashMap<String, List<Row>> rowStaticDataMap;

    public StaticJoinFunction(String joinType, JoinConfig joinConfig, String outputTableId) {
        this.joinType = joinType;
        this.joinConfig = joinConfig;
        this.outputTableId = outputTableId;
        inputTable1 = joinConfig.getTableName();
        inputTable2 = joinConfig.getJoinTableName();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        meta = new MysqlUtil();
        inputTable1Schema = JSONArray.parseArray(meta.getMetaColumnById(inputTable1, MysqlUtil.MyMeta.schema));
        inputTable2Schema = JSONArray.parseArray(meta.getMetaColumnById(inputTable2, MysqlUtil.MyMeta.schema));
        outputTableSchema = JSONArray.parseArray(meta.getMetaColumnById(outputTableId, MysqlUtil.MyMeta.schema));
        rowLen = outputTableSchema.size();
        // 加载静态数据
        List<Row> rowList = PipeLine.getPipeLineStaticData(inputTable2);
        rowStaticDataMap = new HashMap<>();
        String key2 = joinConfig.getOnJoinField();
        for (Row r : rowList) {
            String keyValue = JoinUtil.getKey(key2, r, inputTable2Schema);
            if (rowStaticDataMap.containsKey(keyValue)) {
                rowStaticDataMap.get(keyValue).add(r);
            } else {
                ArrayList<Row> arrayList = new ArrayList<>();
                arrayList.add(r);
                rowStaticDataMap.put(keyValue, arrayList);
            }
        }

    }

    @Override
    public void close() throws Exception {
        meta.close();
    }

    private void buildRowTime(Row outputRow, Object eventTime) {
        /*
        前三个变量：event_time，window_start，window_end
        无窗口的 join，window_start = window_end = event_time
         */
        outputRow.setField(0, JoinUtil.getEventTime(eventTime));
        outputRow.setField(1, JoinUtil.toTimeStr(eventTime));
        outputRow.setField(2, JoinUtil.toTimeStr(eventTime));
    }

    private void buildJoinRow(Row outputRow, Row r1, Row r2) {
        // 和流 join 的区别，r1 != null
        // 右边是否补 null
        boolean isFillRightNull = (r2 == null);
        for (int i = 3; i < rowLen; i++) {
            JSONObject jsonObj = (JSONObject) outputTableSchema.get(i);
            String fieldName = jsonObj.getString(FIELD_NAME);
            // fieldName 字段来自哪个表，第几行
            if (JoinUtil.getFieldBelong(fieldName, joinConfig.getSelectTableField()) == 1) {
                // 来自表1
                int dex = JoinUtil.rowIndex(fieldName, inputTable1Schema);
                outputRow.setField(i, r1.getField(dex));
            } else {
                // 来自表2，为空补 null
                if (isFillRightNull) {
                    outputRow.setField(i, NULL_STR);
                } else {
                    int dex = JoinUtil.rowIndex(fieldName, inputTable2Schema);
                    outputRow.setField(i, r2.getField(dex));
                }
            }
        }
    }

    @Override
    public void flatMap(Row input, Collector<Row> out) throws Exception {
        // 找到被 join 的静态表
        String key1Value = JoinUtil.getKey(joinConfig.getOnTableField(), input, inputTable1Schema);
        List<Row> staticRowList = null;
        if (rowStaticDataMap.containsKey(key1Value)) {
            staticRowList = rowStaticDataMap.get(key1Value);
        }
        if (joinType.equals(LEFT_JOIN)) {
            if (staticRowList == null) {
                // 补齐 null
                Row outputRow = new Row(rowLen);
                this.buildRowTime(outputRow, input.getField(0));
                this.buildJoinRow(outputRow, input, null);
                out.collect(outputRow);
            } else {
                for (Row r2 : staticRowList) {
                    Row outputRow = new Row(rowLen);
                    this.buildRowTime(outputRow, input.getField(0));
                    this.buildJoinRow(outputRow, input, r2);
                    out.collect(outputRow);
                }
            }
        } else {
            // inner join
            if (staticRowList != null) {
                for (Row r2 : staticRowList) {
                    Row outputRow = new Row(rowLen);
                    this.buildRowTime(outputRow, input.getField(0));
                    this.buildJoinRow(outputRow, input, r2);
                    out.collect(outputRow);
                }
            }
        }
    }
}
