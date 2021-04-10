package flink.transform;

import flink.PipeLine;
import flink.source.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.Duration;

// 注册表
public class RegisterUtil {
    // 分为无窗的计算和有窗的计算
    // 有窗口的计算注册表需要指定时间
    // TODO 如果计算下游无窗口，不需要指定时间
    public enum TransformType {
        TRANSFORM_WINDOW,
        TRANSFORM_NO_WINDOW
    }

    public static void registerFlinkTable(TransformType transformType,
                                          StreamTableEnvironment tEnv,
                                          String tableId,
                                          DataStream<Row> dataStream) {
        if (transformType.equals(TransformType.TRANSFORM_NO_WINDOW)) {
            tEnv.createTemporaryView(tableId, dataStream, SourceUtil.getExpressionArray(tableId));
            PipeLine.setPipeLineRowStream(tableId, dataStream);
        } else {
            DataStream<Row> eventTimeDataStream = dataStream
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
            tEnv.createTemporaryView(tableId, eventTimeDataStream, SourceUtil.getExpressionArray(tableId));
            PipeLine.setPipeLineRowStream(tableId, eventTimeDataStream);
        }
    }

}
