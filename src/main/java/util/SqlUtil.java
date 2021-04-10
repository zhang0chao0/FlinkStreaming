package util;


import exception.exceptions.NotQueryException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * SQL 解析工具类
 * https://blog.csdn.net/ciqingloveless/article/details/82626304
 * https://www.cnblogs.com/zhihuifan10/articles/11124953.html
 */
public class SqlUtil {

    // SQL 类型
    enum SqlType {
        ALTER,
        CREATEINDEX,
        CREATETABLE,
        CREATEVIEW,
        DELETE,
        DROP,
        EXECUTE,
        INSERT,
        MERGE,
        REPLACE,
        SELECT,
        TRUNCATE,
        UPDATE,
        UPSERT,
        NOTDEFINE
    }

    private static final Logger LOGGER = LoggerFactory.getLogger("business");

    private final String sql;
    private final Statement sqlStatement;
    // 只针对查询语句
    private Select select;
    private final SelectBody selectBody;
    private final PlainSelect plainSelect;

    public SqlUtil(String sql) throws JSQLParserException {
        this.sql = sql;
        try {
            this.sqlStatement = CCJSqlParserUtil.parse(sql);
            if (this.getSqlType() != SqlType.SELECT) {
                LOGGER.error("非SELECT SQL");
                throw new NotQueryException();
            }
            this.select = (Select) sqlStatement;
            this.selectBody = this.select.getSelectBody();
            this.plainSelect = (PlainSelect) this.selectBody;
            for (SelectItem item : this.getSelectItems()) {
                if (item instanceof AllColumns) {
                    LOGGER.error("不支持 SELECT * 语句!");
                }
            }
        } catch (JSQLParserException ex) {
            LOGGER.error("SQL解析异常，请检查SQL语句：" + ex.getMessage());
            throw ex;
        }
    }

    public String getSql() {
        return sql;
    }

    public Select getSelect() {
        return select;
    }

    public void setSelect(Select select) {
        this.select = select;
    }

    // 当前 SQL 语句类型
    private SqlType getSqlType() {
        if (sqlStatement instanceof Alter) {
            return SqlType.ALTER;
        } else if (sqlStatement instanceof CreateIndex) {
            return SqlType.CREATEINDEX;
        } else if (sqlStatement instanceof CreateTable) {
            return SqlType.CREATETABLE;
        } else if (sqlStatement instanceof CreateView) {
            return SqlType.CREATEVIEW;
        } else if (sqlStatement instanceof Delete) {
            return SqlType.DELETE;
        } else if (sqlStatement instanceof Drop) {
            return SqlType.DROP;
        } else if (sqlStatement instanceof Execute) {
            return SqlType.EXECUTE;
        } else if (sqlStatement instanceof Insert) {
            return SqlType.INSERT;
        } else if (sqlStatement instanceof Merge) {
            return SqlType.MERGE;
        } else if (sqlStatement instanceof Replace) {
            return SqlType.REPLACE;
        } else if (sqlStatement instanceof Select) {
            return SqlType.SELECT;
        } else if (sqlStatement instanceof Truncate) {
            return SqlType.TRUNCATE;
        } else if (sqlStatement instanceof Update) {
            return SqlType.UPDATE;
        } else if (sqlStatement instanceof Upsert) {
            return SqlType.UPSERT;
        } else {
            return SqlType.NOTDEFINE;
        }
    }

    // 获取表名
    public List<String> getTables() {
        return new TablesNamesFinder().getTableList(sqlStatement);
    }

    // 获取 Join
    public List<Join> getJoins() {
        return plainSelect.getJoins();
    }

    public GroupByElement getGroupBy() {
        return plainSelect.getGroupBy();
    }

    // 获取查询字段
    public List<SelectItem> getSelectItems() {
        return plainSelect.getSelectItems();
    }

    // 增加查询字段，字段名，别名，每次增加的字段在最前面
    // id as myID
    public void addSelectColumn(String expression, String alias) {
        if (StringUtils.isNullOrWhitespaceOnly(expression)) {
            return;
        }
        List<SelectItem> copySelectItem = plainSelect.getSelectItems();
        Expression addEx = new Column(expression);
        Alias addAl = null;
        if (!StringUtils.isNullOrWhitespaceOnly(alias)) {
            addAl = new Alias(alias);
        }
        SelectExpressionItem selectExpressionItem = new SelectExpressionItem(addEx).withAlias(addAl);
        // 每次放到最前面!
        copySelectItem.add(0, selectExpressionItem);
        plainSelect.setSelectItems(copySelectItem);
    }

    // 增加 group by 字段
    public void addGroupByColumn(String groupBy) {
        if (StringUtils.isNullOrWhitespaceOnly(groupBy)) {
            return;
        }
        Expression addEx = new Column(groupBy);
        plainSelect.addGroupByColumnReference(addEx);
    }

    public Table getFromTable() {
        return (Table) plainSelect.getFromItem();
    }

    public String toSqlString() {
        return plainSelect.toString();
    }

}
