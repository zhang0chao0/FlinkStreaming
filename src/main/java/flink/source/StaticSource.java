package flink.source;


import flink.PipeLine;
import org.apache.flink.types.Row;
import util.MysqlUtil;

import java.util.List;

/*
静态数据源
 */
public class StaticSource {
    public static void addSource(String inputTable, String table) {
        List<Row> rowList = new MysqlUtil().getStaticData(inputTable, table);
        PipeLine.setPipeLineStaticData(inputTable, rowList);
    }

}
