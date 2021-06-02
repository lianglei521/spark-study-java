package spark.study.java.applog;

import java.io.Serializable;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/02/06.
 * 日志访问信息类--序列化类
 */
public class AccessLogInfo implements Serializable{
    private long timestamp;//时间戳
    private long upTraffic;//上行流量
    private long downTraffic;//下行流量
    public AccessLogInfo(){}
    public AccessLogInfo(long timestamp,long upTraffic,long downTraffic){
        this.timestamp = timestamp;
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getUpTraffic() {
        return upTraffic;
    }

    public void setUpTraffic(long upTraffic) {
        this.upTraffic = upTraffic;
    }

    public long getDownTraffic() {
        return downTraffic;
    }

    public void setDownTraffic(long downTraffic) {
        this.downTraffic = downTraffic;
    }
}
