package flink.transform;

import com.alibaba.fastjson.JSONObject;
import flink.PipeLine;
import flink.function.BuildRowFunction;
import flink.source.SourceUtil;
import flink.sqlbuild.JoinConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.MysqlUtil;


import static flink.source.SourceUtil.STREAM;
import static flink.sqlbuild.SqlBuild.SINGLE_NO_WINDOW;


public class DataStreamTransform {

    private static final Logger LOGGER = LoggerFactory.getLogger("business");

    /*
    输入 outputTableId
    注册表 tEnv.createTemporaryView
     */
    public static void transform(StreamTableEnvironment tEnv, String outputTableId, String selectType) {
        /*
        如果类型是 SINGLE，单表输入，直接执行 sql 即可
         */
        MysqlUtil mysqlUtil = new MysqlUtil();
        String sql = mysqlUtil.getTableSqlById(outputTableId, MysqlUtil.MyTableSql.select_sql);
        Table tempTable = tEnv.sqlQuery(sql);
        DataStream<Row> tempTableStream = tEnv
                .toAppendStream(tempTable, Row.class)
                .map(new BuildRowFunction(outputTableId))
                .name("transform=" + outputTableId)
                .returns(SourceUtil.getRowTypeInfo(outputTableId));
        RegisterUtil.TransformType transformType = null;
        if (selectType.equals(SINGLE_NO_WINDOW)) {
            transformType = RegisterUtil.TransformType.TRANSFORM_WINDOW;
        } else {
            transformType = RegisterUtil.TransformType.TRANSFORM_WINDOW;
        }
        RegisterUtil.registerFlinkTable(
                transformType,
                tEnv,
                outputTableId,
                tempTableStream
        );
    }

    public static void transformJoin(StreamTableEnvironment tEnv, String outputTableId, String joinType, int windowSize) {
        /*
        Join 类型
         */
        MysqlUtil mysqlUtil = new MysqlUtil();
        String joinConfigStr = mysqlUtil.getTableSqlById(outputTableId, MysqlUtil.MyTableSql.select_sql);
        JoinConfig joinConfig = JSONObject.parseObject(joinConfigStr, JoinConfig.class);
        // 表1转流
        String inputTable1 = joinConfig.getTableName();
        DataStream<Row> inputTable1Stream = PipeLine.getPipeLineRowStream(tEnv, inputTable1);
        // 表2
        String inputTable2 = joinConfig.getJoinTableName();
        String table2Type = mysqlUtil.getMetaColumnById(inputTable2, MysqlUtil.MyMeta.table_type);
        DataStream<Row> joinRowStream = null;
        RegisterUtil.TransformType transformType = null;
        if (table2Type.equals(STREAM)) {
            // 流与流的 join
            DataStream<Row> inputTable2Stream = PipeLine.getPipeLineRowStream(tEnv, inputTable2);
            joinRowStream = inputTable1Stream
                    .coGroup(inputTable2Stream)
                    .where(new RowKeySelector(inputTable1, joinConfig.getOnTableField()))
                    .equalTo(new RowKeySelector(inputTable2, joinConfig.getOnJoinField()))
                    .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                    .apply(new StreamJoinFunction(joinType, windowSize, joinConfig, outputTableId))
                    // map 函数啥也没做，主要是要加 returns，不然不能转化为表
                    .map(new BuildRowFunction(outputTableId))
                    .name("transform=" + outputTableId)
                    .returns(SourceUtil.getRowTypeInfo(outputTableId));
            transformType = RegisterUtil.TransformType.TRANSFORM_WINDOW;
        } else {
            // 流与静态表的 join
            joinRowStream = inputTable1Stream
                    .flatMap(new StaticJoinFunction(joinType, joinConfig, outputTableId))
                    .name("transform=" + outputTableId)
                    .returns(SourceUtil.getRowTypeInfo(outputTableId));
            transformType = RegisterUtil.TransformType.TRANSFORM_WINDOW;
        }
        RegisterUtil.registerFlinkTable(
                transformType,
                tEnv,
                outputTableId,
                joinRowStream
        );
    }
}
