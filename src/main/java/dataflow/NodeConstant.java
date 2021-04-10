package dataflow;

import java.util.HashSet;
import java.util.Set;

/*
节点常量类
 */
public class NodeConstant {
    // 窗口类型
    public static final String NO_WINDOW = "none";
    public static final String TUMBLE_WINDOW = "tumble";
    public static final String SLIDE_WINDOW = "slide";
    public static final String SESSION_WINDOW = "session";
    public static final Set<String> WINDOW_SET = new HashSet<>();

    static {
        // 初始化
        WINDOW_SET.add(NO_WINDOW);
        WINDOW_SET.add(TUMBLE_WINDOW);
        WINDOW_SET.add(SLIDE_WINDOW);
        WINDOW_SET.add(SESSION_WINDOW);
    }

}
