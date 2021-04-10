package dataflow;

public class ProcessConfig {
    // 窗口类型
    private String windowType;
    // 窗口大小
    private int windowSize;
    // 滑动大小
    private int slideSize;
    // 会话间隔
    private int sessionGap;
    // SQL 语句
    private String sql;

    public ProcessConfig() {

    }

    public String getWindowType() {
        return windowType;
    }

    public void setWindowType(String windowType) {
        this.windowType = windowType;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    public int getSlideSize() {
        return slideSize;
    }

    public void setSlideSize(int slideSize) {
        this.slideSize = slideSize;
    }

    public int getSessionGap() {
        return sessionGap;
    }

    public void setSessionGap(int sessionGap) {
        this.sessionGap = sessionGap;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    @Override
    public String toString() {
        return "ProcessConfig{" +
                "windowType='" + windowType + '\'' +
                ", windowSize=" + windowSize +
                ", slideSize=" + slideSize +
                ", sessionGap=" + sessionGap +
                ", sql='" + sql + '\'' +
                '}';
    }
}
