package dataflow;

import java.util.List;

/*
计算节点，用于接收 json 参数
 */
public class ProcessNode {
    private String name;
    private String outputTable;
    private List<String> inputTableList;
    private ProcessConfig processConfig;


    public ProcessNode() {

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOutputTable() {
        return outputTable;
    }

    public void setOutputTable(String outputTable) {
        this.outputTable = outputTable;
    }

    public List<String> getInputTableList() {
        return inputTableList;
    }

    public void setInputTableList(List<String> inputTableList) {
        this.inputTableList = inputTableList;
    }

    public ProcessConfig getProcessConfig() {
        return processConfig;
    }

    public void setProcessConfig(ProcessConfig processConfig) {
        this.processConfig = processConfig;
    }

    @Override
    public String toString() {
        return "ProcessNode{" +
                "name='" + name + '\'' +
                ", outputTable='" + outputTable + '\'' +
                ", inputTableList=" + inputTableList +
                ", processConfig=" + processConfig +
                '}';
    }

}
