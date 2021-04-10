package util;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONReader;
import dataflow.ProcessNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
读入配置文件
 */
public class ProjectConfigReader {

    public enum kafka_prop {
        kafka_consumer, kafka_producer
    }

    public static final String KAFKA_CONSUMER = "kafka_consumer.properties";
    public static final String KAFKA_PRODUCER = "kafka_producer.properties";
    public static final String DATAFLOW = "dataflow.json";
    public static final String DATA_JSON = "sql_parse.json";
    private static final Logger LOGGER = LoggerFactory.getLogger("business");
    // 提交集群运行需要指定配置文件地址
    private static final String PATH = "/Users/chaoczhang/Desktop/JavaCode/flinkLearn/flinkDemo/target/classes/";


    public static Properties kafkaProperties(kafka_prop prop) {
        Properties properties = new Properties();
        try {
            String filePath = null;
            if (prop.equals(kafka_prop.kafka_consumer)) {
                // filePath = Objects.requireNonNull(ProjectConfigReader.class.getClassLoader().getResource(KAFKA_CONSUMER)).getFile();
                filePath = PATH + KAFKA_CONSUMER;
            } else {
                // filePath = Objects.requireNonNull(ProjectConfigReader.class.getClassLoader().getResource(KAFKA_PRODUCER)).getFile();
                filePath = PATH + KAFKA_PRODUCER;
            }
            FileInputStream kafkaInputStream = new FileInputStream(filePath);
            properties.load(kafkaInputStream);
            kafkaInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("读取kafka配置文件异常：" + e.getMessage());
        }
        return properties;
    }

    public static JSONArray readDataflow() {
        JSONArray result = null;
        try {
            // String dataflow = Objects.requireNonNull(ProjectConfigReader.class.getClassLoader().getResource(DATAFLOW)).getFile();
            String dataflow = PATH + DATAFLOW;
            FileReader fileReader = new FileReader(dataflow);
            JSONReader jsonReader = new JSONReader(fileReader);
            result = JSONArray.parseArray(jsonReader.readString());
        } catch (Exception ex) {
            ex.printStackTrace();
            LOGGER.error("读取dataflow.json异常：" + ex.getMessage());
        }
        return result;
    }

    public static ProcessNode readDataJson() {
        ProcessNode processNode = null;
        try {
            String filePath = Objects.requireNonNull(ProjectConfigReader.class.getClassLoader().getResource(DATA_JSON)).getFile();
            FileReader fileReader = new FileReader(filePath);
            JSONReader jsonReader = new JSONReader(fileReader);
            processNode = JSON.parseObject(jsonReader.readString(), ProcessNode.class);
        } catch (Exception ex) {
            ex.printStackTrace();
            LOGGER.error("读取sql_parse.json异常：" + ex.getMessage());
        }
        return processNode;
    }

}
