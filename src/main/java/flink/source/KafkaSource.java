package flink.source;

import flink.function.Str2RowFunction;
import flink.transform.RegisterUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ProjectConfigReader;

/*
添加数据源
输入：kafka 中的数据，json 格式
输出：DataStream<row>，保存到 dataStreamHashMap
 */
public class KafkaSource {

    private static final Logger LOGGER = LoggerFactory.getLogger("business");

    /*
    inputTable 来自外部接入的数据表，存储位置和 Schema 都可以从元数据获取
     */
    public static void addSource(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String inputTable, String topic) {
        DataStream<Row> sourceRow = env.addSource(
                new FlinkKafkaConsumer<>(
                        topic,
                        new JSONKeyValueDeserializationSchema(true),
                        ProjectConfigReader.kafkaProperties(ProjectConfigReader.kafka_prop.kafka_consumer)
                )
        )
                .map(new Str2RowFunction(inputTable))
                .name("kafka source=" + inputTable)
                .returns(SourceUtil.getRowTypeInfo(inputTable));
        // 注册表
        RegisterUtil.registerFlinkTable(
                RegisterUtil.TransformType.TRANSFORM_WINDOW,
                tEnv,
                inputTable,
                sourceRow
        );
    }
}
