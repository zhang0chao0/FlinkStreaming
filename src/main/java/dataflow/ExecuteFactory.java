package dataflow;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ProjectConfigReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;


/*
组装工具类，通过 ExecuteStreamNode 组装执行队列
确保 output_table 执行顺序：子节点必须在父节点之后执行
 */
public class ExecuteFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger("business");

    private ExecuteFactory() {
    }

    // 从 dataflow.json 中获取数据，保存
    public static HashMap<String, ProcessNode> processNodeMap;
    public static final Set<String> outputTableKeys;
    private static HashMap<String, ExecuteStreamNode> executeStreamNodeMap;

    static {
        buildProcessNodeMap();
        outputTableKeys = processNodeMap.keySet();
        buildExecuteStreamNodeMap();
    }

    // 从 dataflow.json 中获取计算列表
    private static void buildProcessNodeMap() {
        processNodeMap = new HashMap<>();
        JSONArray dataflowArray = null;
        dataflowArray = ProjectConfigReader.readDataflow();
        // 如果为空在此处异常
        assert dataflowArray != null;
        for (Object o : dataflowArray) {
            ProcessNode processNode = JSON.parseObject(o.toString(), ProcessNode.class);
            // TODO 参数校验
            processNodeMap.put(processNode.getOutputTable(), processNode);
        }
    }

    // 获取执行流节点
    private static void buildExecuteStreamNodeMap() {
        // 获取执行流节点
        executeStreamNodeMap = new HashMap<>();
        for (String key : outputTableKeys) {
            ExecuteStreamNode executeNode = new ExecuteStreamNode(key);
            List<String> children = new ArrayList<>();
            // n ^ 2 复杂度，如果节点的输入有它，则该节点为 key 的子节点
            for (ProcessNode node : processNodeMap.values()) {
                if (!node.getOutputTable().equals(key) && node.getInputTableList().contains(key)) {
                    children.add(node.getOutputTable());
                }
            }
            executeNode.setChildren(children);
            // 是否是根节点，input 不在 keySet() 中
            boolean isRoot = true;
            for (String input : processNodeMap.get(key).getInputTableList()) {
                if (outputTableKeys.contains(input)) {
                    isRoot = false;
                    break;
                }
            }
            executeNode.setRoot(isRoot);
            executeStreamNodeMap.put(executeNode.getOutputTable(), executeNode);
        }
    }

    public static Queue<String> getExecuteQueue() {
        // 获取执行队列
        Queue<String> executeQueue = new LinkedList<>();
        // 遍历队列
        Queue<String> tempQueue = new LinkedList<>();
        // 将根节点表名放入队列中
        for (String key : outputTableKeys) {
            if (executeStreamNodeMap.get(key).isRoot()) {
                tempQueue.add(key);
            }
        }
        // 执行主函数
        while (!tempQueue.isEmpty()) {
            String tableName = tempQueue.remove();
            // 开始遍历，放入执行队列
            executeQueue.add(tableName);
            if (executeStreamNodeMap.get(tableName).getChildren().size() != 0) {
                for (String child : executeStreamNodeMap.get(tableName).getChildren()) {
                    if (!tempQueue.contains(child)) {
                        tempQueue.add(child);
                    }
                }
            }
        }
        return executeQueue;
    }
}
