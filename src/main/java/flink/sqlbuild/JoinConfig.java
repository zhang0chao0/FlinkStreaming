package flink.sqlbuild;

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;
import java.util.List;

public class JoinConfig implements Serializable {
    @JSONField(name = "table_name")
    private String tableName;
    @JSONField(name = "join_table_name")
    private String joinTableName;
    @JSONField(name = "on_table_field")
    private String onTableField;
    @JSONField(name = "on_join_field")
    private String onJoinField;
    @JSONField(name = "select_table_field")
    private List<String> selectTableField;
    @JSONField(name = "select_join_field")
    private List<String> selectJoinField;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getJoinTableName() {
        return joinTableName;
    }

    public void setJoinTableName(String joinTableName) {
        this.joinTableName = joinTableName;
    }

    public String getOnTableField() {
        return onTableField;
    }

    public void setOnTableField(String onTableField) {
        this.onTableField = onTableField;
    }

    public String getOnJoinField() {
        return onJoinField;
    }

    public void setOnJoinField(String onJoinField) {
        this.onJoinField = onJoinField;
    }

    public List<String> getSelectTableField() {
        return selectTableField;
    }

    public void setSelectTableField(List<String> selectTableField) {
        this.selectTableField = selectTableField;
    }

    public List<String> getSelectJoinField() {
        return selectJoinField;
    }

    public void setSelectJoinField(List<String> selectJoinField) {
        this.selectJoinField = selectJoinField;
    }

}
