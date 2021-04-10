package flink.transform;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import flink.sqlbuild.JoinConfig;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.commons.compress.utils.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.MysqlUtil;

import java.util.ArrayList;

import static flink.source.SourceUtil.FIELD_NAME;
import static flink.sqlbuild.SqlBuild.LEFT_JOIN;
import static flink.sqlbuild.SqlBuild.RIGHT_JOIN;
import static flink.transform.JoinUtil.NULL_STR;

public class StreamJoinFunction implements CoGroupFunction<Row, Row, Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger("business");

    // inner，left
    private final String joinType;
    private final int windowSize;
    private final JoinConfig joinConfig;
    private final JSONArray inputTable1Schema;
    private final JSONArray inputTable2Schema;
    private final JSONArray outputTableSchema;
    // 一行有多少数据
    private final int rowLen;

    public StreamJoinFunction(String joinType, int windowSize, JoinConfig joinConfig, String outputTableId) {
        this.joinType = joinType;
        this.windowSize = windowSize;
        this.joinConfig = joinConfig;
        String inputTable1 = joinConfig.getTableName();
        String inputTable2 = joinConfig.getJoinTableName();
        MysqlUtil meta = new MysqlUtil();
        inputTable1Schema = JSONArray.parseArray(meta.getMetaColumnById(inputTable1, MysqlUtil.MyMeta.schema));
        inputTable2Schema = JSONArray.parseArray(meta.getMetaColumnById(inputTable2, MysqlUtil.MyMeta.schema));
        outputTableSchema = JSONArray.parseArray(meta.getMetaColumnById(outputTableId, MysqlUtil.MyMeta.schema));
        rowLen = outputTableSchema.size();
    }

    private void buildRowTime(Row outputRow, Object eventTime) {
        /*
        前三个变量：event_time，window_start，window_end
        event_time = window_end
         */
        outputRow.setField(0, JoinUtil.getEventEndTime(eventTime, windowSize));
        outputRow.setField(1, JoinUtil.getWindowStart(eventTime, windowSize));
        outputRow.setField(2, JoinUtil.getWindowEnd(eventTime, windowSize));
    }

    private void buildJoinRow(Row outputRow, Row r1, Row r2) {
        // 左边是否补 null
        boolean isFillLeftNull = (r1 == null);
        // 右边是否补 null
        boolean isFillRightNull = (r2 == null);
        for (int i = 3; i < rowLen; i++) {
            JSONObject jsonObj = (JSONObject) outputTableSchema.get(i);
            String fieldName = jsonObj.getString(FIELD_NAME);
            // fieldName 字段来自哪个表，第几行
            if (JoinUtil.getFieldBelong(fieldName, joinConfig.getSelectTableField()) == 1) {
                // 来自表1
                if (isFillLeftNull) {
                    outputRow.setField(i, NULL_STR);
                } else {
                    int dex = JoinUtil.rowIndex(fieldName, inputTable1Schema);
                    outputRow.setField(i, r1.getField(dex));
                }
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
    public void coGroup(Iterable<Row> first, Iterable<Row> second, Collector<Row> out) throws Exception {
        ArrayList<Row> table1 = Lists.newArrayList(first.iterator());
        ArrayList<Row> table2 = Lists.newArrayList(second.iterator());
        // outputRow 的字段来自 row1 和 row2
        if (joinType.equals(LEFT_JOIN)) {
            if (table1.size() != 0) {
                // LOGGER.info("join event_time 数据类型=" + table1.get(0).getField(0).getClass());
                boolean isRightNull = (table2.size() == 0);
                for (Row r1 : table1) {
                    if (isRightNull) {
                        Row outputRow = new Row(rowLen);
                        this.buildRowTime(outputRow, r1.getField(0));
                        this.buildJoinRow(outputRow, r1, null);
                        out.collect(outputRow);
                    } else {
                        for (Row r2 : table2) {
                            // 补齐其它的字段
                            Row outputRow = new Row(rowLen);
                            this.buildRowTime(outputRow, r1.getField(0));
                            this.buildJoinRow(outputRow, r1, r2);
                            out.collect(outputRow);
                        }
                    }
                }
            }
        } else if (joinType.equals(RIGHT_JOIN)) {
            if (table2.size() != 0) {
                boolean isLeftNull = (table1.size() == 0);
                for (Row r2 : table2) {
                    if (isLeftNull) {
                        Row outputRow = new Row(rowLen);
                        this.buildRowTime(outputRow, r2.getField(0));
                        this.buildJoinRow(outputRow, null, r2);
                        out.collect(outputRow);
                    } else {
                        for (Row r1 : table1) {
                            Row outputRow = new Row(rowLen);
                            this.buildRowTime(outputRow, r1.getField(0));
                            this.buildJoinRow(outputRow, r1, r2);
                            out.collect(outputRow);
                        }
                    }
                }
            }
        } else {
            // inner join
            if (table1.size() != 0 && table2.size() != 0) {
                for (Row r1 : table1) {
                    for (Row r2 : table2) {
                        Row outputRow = new Row(rowLen);
                        this.buildRowTime(outputRow, r1.getField(0));
                        this.buildJoinRow(outputRow, r1, r2);
                        out.collect(outputRow);
                    }
                }
            }
        }
    }
}
