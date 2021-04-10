package dataflow;

import java.util.List;

/*
组装执行队列使用
 */
public class ExecuteStreamNode {
    // 计算得出，是否为执行流的根节点
    private boolean isRoot;
    private String outputTable;
    private List<String> children;

    public ExecuteStreamNode() {

    }

    public ExecuteStreamNode(String outputTable) {
        this.outputTable = outputTable;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public void setRoot(boolean root) {
        isRoot = root;
    }

    public String getOutputTable() {
        return outputTable;
    }

    public void setOutputTable(String outputTable) {
        this.outputTable = outputTable;
    }

    public List<String> getChildren() {
        return children;
    }

    public void setChildren(List<String> children) {
        this.children = children;
    }
}
