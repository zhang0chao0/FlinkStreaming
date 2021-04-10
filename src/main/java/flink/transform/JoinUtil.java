package flink.transform;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static flink.source.SourceUtil.FIELD_NAME;
import static flink.sqlbuild.SqlBuild.TIME_PATTERN;


public class JoinUtil {

    // 如果 row set i = null，那么第 i 行不会显示出来
    // TODO 如果是字符串类型，补 NULL，int 类型补 0
    public static final String NULL_STR = "NULL";

    // key 在 row 的第几行？
    public static String getKey(String keyByValue, Row value, JSONArray schema) {
        // keyByValue 在 Row 的第几行，需要从元数据中知道
        int index = 0;
        for (Object obj : schema) {
            JSONObject jsonObject = (JSONObject) obj;
            if (jsonObject.getString(FIELD_NAME).equals(keyByValue)) {
                break;
            }
            index++;
        }
        return (String) value.getField(index);
    }

    public static int getFieldBelong(String field, List<String> table1) {
        // 确定字段在哪个表中
        for (String item : table1) {
            if (item.equals(field)) {
                return 1;
            }
        }
        return 2;
    }

    public static int rowIndex(String field, JSONArray schema) {
        int index = 0;
        for (Object obj : schema) {
            JSONObject jsonObject = (JSONObject) obj;
            if (jsonObject.getString(FIELD_NAME).equals(field)) {
                return index;
            }
            index++;
        }
        return -1;
    }

    // 计算 join 后的窗口 window_start、window_end
    private static long getWindowStartMills(Object eventTime, int windowSize) {
        if (eventTime instanceof java.sql.Timestamp) {
            // 秒级别时间戳
            long second = ((Timestamp) eventTime).getTime() / 1000;
            // 丢失精度
            long s1 = second / windowSize;
            return s1 * windowSize * 1000;
        } else {
            return 0L;
        }
    }

    public static Object getEventEndTime(Object eventTime, int windowSize) {
        if (eventTime instanceof java.sql.Timestamp) {
            long start = getWindowStartMills(eventTime, windowSize);
            long end = start + windowSize * 1000L;
            return new Timestamp(end);
        }
        return null;
    }

    public static String getWindowStart(Object eventTime, int windowSize) {
        if (eventTime instanceof java.sql.Timestamp) {
            long start = getWindowStartMills(eventTime, windowSize);
            return new SimpleDateFormat(TIME_PATTERN).format(new Date(start));
        } else {
            return "";
        }
    }

    public static String getWindowEnd(Object eventTime, int windowSize) {
        if (eventTime instanceof java.sql.Timestamp) {
            long start = getWindowStartMills(eventTime, windowSize);
            long end = start + windowSize * 1000L;
            return new SimpleDateFormat(TIME_PATTERN).format(new Date(end));
        } else {
            return "";
        }
    }

    public static Object getEventTime(Object eventTime) {
        if (eventTime instanceof java.sql.Timestamp) {
            return eventTime;
        }
        return null;
    }

    public static String toTimeStr(Object eventTime) {
        if (eventTime instanceof java.sql.Timestamp) {
            long mills = ((Timestamp) eventTime).getTime();
            return new SimpleDateFormat(TIME_PATTERN).format(new Date(mills));
        } else {
            return "";
        }
    }

}
