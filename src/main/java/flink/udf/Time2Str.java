package flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static flink.sqlbuild.SqlBuild.TIME_PATTERN;

/*
window_start，window_end 字段需要转化为 String
顺便 + 8 小时
 */
public class Time2Str extends ScalarFunction {

    public String eval(LocalDateTime localDateTime) {
        return localDateTime.plusHours(8).format(DateTimeFormatter.ofPattern(TIME_PATTERN));
    }

}
