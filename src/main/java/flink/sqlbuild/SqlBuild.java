package flink.sqlbuild;

import dataflow.NodeConstant;
import dataflow.ProcessConfig;
import dataflow.ProcessNode;
import flink.PipeLine;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.MysqlUtil;
import util.ProjectConfigReader;
import util.SqlUtil;


import static dataflow.NodeConstant.WINDOW_SET;

/*
SQL 预校验，构建 flink sql
 */
public class SqlBuild {
    /*
    flink sql 里面只能有一个 rowtime 字段!!!
    window_start，window_end 字段需要转化为 String
     */
    public static final String WINDOW_START = "window_start";
    public static final String WINDOW_END = "window_end";
    // 约定数据必有记录时间的字段 event_time
    public static final String EVENT_TIME = "event_time";
    public static final String EVENT_TIME_TO_STR = "time2str(event_time)";
    public static final String INNER_JOIN = "inner_join";
    public static final String LEFT_JOIN = "left_join";
    public static final String RIGHT_JOIN = "right_join";
    public static final String SINGLE_NO_WINDOW = "single_no_window";
    public static final String SINGLE_WINDOW = "single_window";
    public static final String TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    private static final Logger LOGGER = LoggerFactory.getLogger("business");
    public static final int TABLE_MAX = 2;

    // 成员变量
    SqlUtil sqlUtil;
    ProcessNode processNode;
    final StreamExecutionEnvironment env;
    final StreamTableEnvironment tEnv;

    public SqlBuild(ProcessNode processNode) throws JSQLParserException {
        this.processNode = processNode;
        String sql = processNode.getProcessConfig().getSql();
        sqlUtil = new SqlUtil(sql);
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        PipeLine.registerUDF(tEnv);
    }

    // 是否是 group by 语句
    public boolean isGroupBy() {
        return sqlUtil.getGroupBy() != null;
    }

    // 是否是 group by 某字段
    public boolean isGroupByColumn(String column) {
        if (isGroupBy()) {
            for (Expression ex : sqlUtil.getGroupBy().getGroupByExpressions()) {
                if (column.equals(ex.toString())) {
                    return true;
                }
            }
            return false;
        } else {
            return false;
        }
    }

    // 是否是 group by 语句
    public boolean isJoin() {
        return sqlUtil.getJoins() != null;
    }

    /*
    无窗口，只能是 where 过滤任务
    有窗口，基于窗口的操作，group by、聚合函数、join 操作
     */
    public void configCheck() {
        ProcessConfig curConfig = processNode.getProcessConfig();
        String windowType = curConfig.getWindowType();
        if (!WINDOW_SET.contains(windowType)) {
            LOGGER.error("不支持的窗口类型，仅支持类型=" + WINDOW_SET.toString());
        }
        String sql = curConfig.getSql();
        if (StringUtils.isNullOrWhitespaceOnly(sql)) {
            LOGGER.error("SQL语句不能为空");
        }
        if (sqlUtil.getTables().size() > TABLE_MAX) {
            LOGGER.error("不支持两个表以上的查询语句");
        }
        if (windowType.equals(NodeConstant.NO_WINDOW)) {
            if (isGroupBy()) {
                LOGGER.error("SQL错误，无窗口查询不支持 group by 语句");
            }
            // TODO 其它校验逻辑
            // 不能与 event_time 字段冲突等
            // schema 校验
        }
    }

    public void buildSql() {
        String selectType = null;
        String sqlConfig = null;
        if (isJoin()) {
            // 只支持 inner、left、right join
            if (sqlUtil.getJoins().size() > 1) {
                LOGGER.error("不支持三个表以及以上的join操作!");
            }
            Join join = sqlUtil.getJoins().get(0);
            if (!(join.isInner() || join.isLeft() || join.isRight())) {
                LOGGER.error("仅支持 inner join, left join, right join 操作!");
            }
            if (join.isLeft()) {
                selectType = LEFT_JOIN;
            } else if (join.isRight()) {
                selectType = RIGHT_JOIN;
            } else {
                selectType = INNER_JOIN;
            }
            sqlConfig = ConfigUtil.buildJoinConfig(processNode.getOutputTable(), sqlUtil);
        } else {
            /*
            对简单的 sql 进行处理
            无窗口：select a, b, c from table1;
            滚动窗口：select a,b,c,sum(d) as d_count from table1 group by a,b,c
                group by 窗口后，对所有 select 字段都要加上 group by
            其它窗口同理
             */
            String window_type = processNode.getProcessConfig().getWindowType();
            if (window_type.equals(NodeConstant.NO_WINDOW)) {
                selectType = SINGLE_NO_WINDOW;
                // 为了统一处理，无窗口也加上 window_start、window_end = event_time
                // 倒序添加，保证 EVENT_TIME 是第一个
                sqlUtil.addSelectColumn(EVENT_TIME_TO_STR, WINDOW_END);
                sqlUtil.addSelectColumn(EVENT_TIME_TO_STR, WINDOW_START);
                // 加上事件时间
                sqlUtil.addSelectColumn(EVENT_TIME, null);
            } else {
                // 如果 select id, 必须有 group by id
                for (SelectItem item : sqlUtil.getSelectItems()) {
                    SelectExpressionItem selectExpressionItem = (SelectExpressionItem) item;
                    Expression expression = selectExpressionItem.getExpression();
                    if (expression instanceof Function) {
                        // 跳过函数
                        continue;
                    }
                    String ex = expression.toString();
                    if (!isGroupByColumn(ex)) {
                        LOGGER.error("SQL异常，字段：" + ex + "未被加入 group by!");
                    }
                }
                // 窗口的结束时间作为输出数据的 EVENT_TIME
                String windowStart = null;
                String windowEnd = null;
                selectType = SINGLE_WINDOW;
                if (window_type.equals(NodeConstant.TUMBLE_WINDOW)) {
                    // 滚动窗口
                    int windowSize = processNode.getProcessConfig().getWindowSize();
                    if (windowSize < 1) {
                        LOGGER.error("滚动窗口大小不能小于1秒");
                    }
                    String base = "event_time, INTERVAL '" + windowSize + "' second";
                    windowStart = "TUMBLE_START(" + base + ")";
                    windowEnd = "TUMBLE_END(" + base + ")";
                    String tumble = "TUMBLE(" + base + ")";
                    sqlUtil.addGroupByColumn(tumble);
                } else if (window_type.equals(NodeConstant.SLIDE_WINDOW)) {
                    // 滑动窗口
                    int windowSize = processNode.getProcessConfig().getWindowSize();
                    int slideSize = processNode.getProcessConfig().getSlideSize();
                    if (windowSize < 1 || slideSize < 1) {
                        LOGGER.error("滑动窗口大小和滑动频率不能小于1秒");
                    }
                    String base = "event_time, INTERVAL '" + slideSize + "' second, INTERVAL '" + windowSize + "' second";
                    windowStart = "HOP_START(" + base + ")";
                    windowEnd = "HOP_END(" + base + ")";
                    String slide = "HOP(" + base + ")";
                    sqlUtil.addGroupByColumn(slide);
                } else {
                    // 会话窗口
                    int sessionGap = processNode.getProcessConfig().getSessionGap();
                    if (sessionGap < 1) {
                        LOGGER.error("会话窗口的会话间隙不能小于1秒");
                    }
                    String base = "event_time, INTERVAL '" + sessionGap + "' second";
                    windowStart = "SESSION_START(" + base + ")";
                    windowEnd = "SESSION_END(" + base + ")";
                    String gap = "SESSION(" + base + ")";
                    sqlUtil.addGroupByColumn(gap);
                }
                String windowEndStr = "time2str(" + windowEnd + ")";
                String windowStartStr = "time2str(" + windowStart + ")";
                sqlUtil.addSelectColumn(windowEndStr, WINDOW_END);
                sqlUtil.addSelectColumn(windowStartStr, WINDOW_START);
                // String longEventTime = "time2long(" + windowEnd + ")";
                sqlUtil.addSelectColumn(windowEnd, EVENT_TIME);
            }
            sqlConfig = sqlUtil.toSqlString();
            ConfigUtil.buildSqlSchema(
                    env,
                    tEnv,
                    processNode,
                    sqlConfig
            );
        }
        MysqlUtil db = new MysqlUtil();
        db.insertOrUpdateTableSql(
                processNode.getOutputTable(),
                selectType,
                sqlConfig
        );
        db.close();
    }


    public static void main(String[] args) throws Exception {
        ProcessNode processNode = ProjectConfigReader.readDataJson();
        SqlBuild sqlBuild = new SqlBuild(processNode);
        sqlBuild.configCheck();
        sqlBuild.buildSql();
    }

}
