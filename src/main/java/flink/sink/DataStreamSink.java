package flink.sink;

import com.alibaba.fastjson.JSONObject;
import flink.PipeLine;
import flink.function.Row2StrFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.MysqlUtil;
import util.ProjectConfigReader;


import static flink.source.SourceUtil.KAFKA;
import static flink.source.SourceUtil.TOPIC;


public class DataStreamSink {

    private static final Logger LOGGER = LoggerFactory.getLogger("business");

    /*
    从元数据中找对应输出，有则添加
     */
    public static void sink(StreamTableEnvironment tEnv, String outputTableId) {
        MysqlUtil meta = new MysqlUtil();
        JSONObject storages = (JSONObject) JSONObject.parse(meta.getMetaColumnById(outputTableId, MysqlUtil.MyMeta.storages));
        if (storages.getJSONObject(KAFKA) != null) {
            LOGGER.info("组装输出 kafka，table_id=" + outputTableId);
            // 表转流，输出；若流存在则不转
            DataStream<Row> tStream = PipeLine.getPipeLineRowStream(tEnv, outputTableId);
            // tStream.print();
            String topic = storages.getJSONObject(KAFKA).getString(TOPIC);
            tStream.map(new Row2StrFunction(outputTableId))
                    .addSink(
                            new FlinkKafkaProducer<>(
                                    topic,
                                    new SimpleStringSchema(),
                                    ProjectConfigReader.kafkaProperties(ProjectConfigReader.kafka_prop.kafka_producer)
                            )
                    );
        }
    }
}
