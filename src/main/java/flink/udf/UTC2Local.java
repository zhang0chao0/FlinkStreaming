package flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDateTime;


/*
flink sql 计算后的时间提前 8 小时，用自定义函数补上
TODO 计算后的类型是 Timestamp(9) 类型，rowtime() 类型需要 Timestamp(3) 类型
内存中计算的时候不需要给它转时间，只有 sink 输出的时候才把时间转成正确的
现在的逻辑，自带输出，在输出的时候做转移，以后若元数据中有 kafka 输出，拼接 SQL 的时候加上这个 UDF 即可
 */
public class UTC2Local extends ScalarFunction {

    public LocalDateTime eval(LocalDateTime localDateTime) {

        return localDateTime.plusHours(8);
    }

}
