package flink;

import dataflow.ExecuteFactory;
import dataflow.ProcessNode;
import flink.sink.DataStreamSink;
import flink.source.SourceUtil;
import flink.transform.DataStreamTransform;
import flink.udf.Time2Str;
import flink.udf.UTC2Local;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.MysqlUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import static flink.sqlbuild.SqlBuild.SINGLE_NO_WINDOW;
import static flink.sqlbuild.SqlBuild.SINGLE_WINDOW;


public class PipeLine {
    private static final Logger LOGGER = LoggerFactory.getLogger("business");
    /*
    保存表名，以及对应的流表
    执行队列保证了子节点一定在父节点之后执行，所以依次执行是可以找到的
    对于外部输入的表，提前从 kafka 读入保存即可
    单表操作基于表，多表 join 基于流，存起来避免循环转换
     */
    // 流
    private static final HashMap<String, DataStream<Row>> pipeLineRowStreamMap;
    // 静态数据
    private static final HashMap<String, List<Row>> pipeLineStaticDataMap;
    // 从 dataflow.json 中读出的配置文件
    public static final HashMap<String, ProcessNode> processNodeHashMap;
    public static final Set<String> outputTableKeys;

    static {
        pipeLineRowStreamMap = new HashMap<>();
        pipeLineStaticDataMap = new HashMap<>();
        processNodeHashMap = ExecuteFactory.processNodeMap;
        outputTableKeys = ExecuteFactory.outputTableKeys;
    }

    public static DataStream<Row> getPipeLineRowStream(StreamTableEnvironment tEnv, String key) {
        return pipeLineRowStreamMap.get(key);
    }

    public static void setPipeLineRowStream(String key, DataStream<Row> value) {
        pipeLineRowStreamMap.put(key, value);
    }

    public static List<Row> getPipeLineStaticData(String key) {
        return pipeLineStaticDataMap.get(key);
    }

    public static void setPipeLineStaticData(String key, List<Row> value) {
        pipeLineStaticDataMap.put(key, value);
    }


    public static void registerUDF(StreamTableEnvironment tEnv) {
        LOGGER.info("注册UDF");
        tEnv.createTemporarySystemFunction("utc2local", new UTC2Local());
        tEnv.createTemporarySystemFunction("time2str", new Time2Str());
    }

    // 获取外部数据源，保存到 pipeLineMap 中
    public static void source(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        for (String key : outputTableKeys) {
            for (String input : processNodeHashMap.get(key).getInputTableList()) {
                if (!outputTableKeys.contains(input) && !pipeLineRowStreamMap.containsKey(input)) {
                    // input 不属于 outputTableKeys，那一定是数据源
                    // 并且没有加载过，防止多个计算任务引用同一个数据源重复加载
                    LOGGER.info("组装数据源，table_id=" + input);
                    SourceUtil.addSource(env, tEnv, input);
                }
            }
        }
    }

    public static void transform(StreamTableEnvironment tEnv) {
        // 开始执行
        Queue<String> executeQueue = ExecuteFactory.getExecuteQueue();
        LOGGER.info("组装执行队列，队列=" + executeQueue.toString());
        while (!executeQueue.isEmpty()) {
            String outputTable = executeQueue.remove();
            transform(tEnv, outputTable);
        }
    }

    public static void transform(StreamTableEnvironment tEnv, String outputTableId) {
        LOGGER.info("组装计算表，table_id=" + outputTableId);
        MysqlUtil mysqlUtil = new MysqlUtil();
        String selectType = mysqlUtil.getTableSqlById(outputTableId, MysqlUtil.MyTableSql.select_type);
        if (selectType.equals(SINGLE_NO_WINDOW) || selectType.equals(SINGLE_WINDOW)) {
            // 单表查询，只有一个输入
            DataStreamTransform.transform(tEnv, outputTableId, selectType);
        } else {
            // join
            DataStreamTransform.transformJoin(tEnv,
                    outputTableId,
                    selectType,
                    processNodeHashMap.get(outputTableId).getProcessConfig().getWindowSize()
            );
        }
    }

    public static void sink(StreamTableEnvironment tEnv) {
        // 遍历 pipeLineMap，元数据有输出则输出
        for (String key : outputTableKeys) {
            DataStreamSink.sink(tEnv, key);
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 开启 checkout point
        env.enableCheckpointing(1000);
        /*
        执行 PineLine 之前，先执行 sqlbuild.SqlBuild 解析 sql、信息存元数据
        1. 注册 udf 函数
        2. 添加数据源
        3. 添加计算
        4. 添加输出
         */
        // 注册 UDF 的 tEnv 必须和 tEnv.sqlQuery 的是同一个，否则找不到函数
        registerUDF(tEnv);
        source(env, tEnv);
        transform(tEnv);
        sink(tEnv);
        // 执行
        LOGGER.info("执行任务......");
        env.execute();
        // 执行计划
        // System.out.println(env.getExecutionPlan());
    }
}
