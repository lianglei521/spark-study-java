package spark.study.java.applog;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/02/06.
 * 日志的二次排序key
 */
public class AccessLogSortKey implements Ordered<AccessLogSortKey>,Serializable{
    private long upTraffic;
    private long downTraffic;
    private long timestamp;

    @Override
    public int compare(AccessLogSortKey other) {
        if(upTraffic!=other.upTraffic){
            return (int)(upTraffic - other.upTraffic);
        }else if(downTraffic != other.downTraffic){
            return (int)(downTraffic - other.downTraffic);
        }else if(timestamp != other.timestamp){
            return (int)(timestamp - other.timestamp);
        }
        return 0;
    }

    @Override
    public boolean $less(AccessLogSortKey other) {
        if(upTraffic < other.upTraffic){
            return true;
        }else if(upTraffic == other.upTraffic && downTraffic < other.downTraffic){
            return true;
        }else if(upTraffic == other.upTraffic && downTraffic == other.downTraffic && timestamp < other.timestamp ){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(AccessLogSortKey other) {
        if(upTraffic > other.upTraffic){
            return true;
        }else if(upTraffic == other.upTraffic && downTraffic > other.downTraffic){
            return true;
        }else if(upTraffic == other.upTraffic && downTraffic == other.downTraffic && timestamp > other.timestamp ){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(AccessLogSortKey other) {
        if($less(other)){
            return true;
        }else if(upTraffic == other.upTraffic && downTraffic == other.downTraffic && timestamp == other.timestamp){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(AccessLogSortKey other) {
        if($greater(other)){
            return true;
        }else if(upTraffic == other.upTraffic && downTraffic == other.downTraffic && timestamp == other.timestamp){
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(AccessLogSortKey other) {
        if(upTraffic!=other.upTraffic){
            return (int)(upTraffic - other.upTraffic);
        }else if(downTraffic != other.downTraffic){
            return (int)(downTraffic - other.downTraffic);
        }else if(timestamp != other.timestamp){
            return (int)(timestamp - other.timestamp);
        }
        return 0;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AccessLogSortKey)) return false;

        AccessLogSortKey that = (AccessLogSortKey) o;

        if (getUpTraffic() != that.getUpTraffic()) return false;
        if (getDownTraffic() != that.getDownTraffic()) return false;
        return getTimestamp() == that.getTimestamp();
    }

    @Override
    public int hashCode() {
        int result = (int) (getUpTraffic() ^ (getUpTraffic() >>> 32));
        result = 31 * result + (int) (getDownTraffic() ^ (getDownTraffic() >>> 32));
        result = 31 * result + (int) (getTimestamp() ^ (getTimestamp() >>> 32));
        return result;
    }

    public long getTimestamp() {

        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "upTraffic=" + upTraffic +
                ", downTraffic=" + downTraffic +
                ", timestamp=" + timestamp ;
    }
}
