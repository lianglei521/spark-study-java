package spark.study.java.core;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * author:liangsir
 * qq:714628767
 * created 2018/12/29.
 * 自定义二次排序key
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>,Serializable{

    private  static final long seriaVersionUID = -2366006422945129991L;

    private int first;
    private int second;

    public SecondarySortKey(int first,int second){
        this.first = first;
        this.second = second;
    }


    @Override
    public int compare(SecondarySortKey that) {
        if(this.first != that.getFirst()){
            return this.first - that.getFirst();
        }else{
            return this.second -that.getSecond();
        }
    }

    @Override
    public boolean $less(SecondarySortKey that) {
        if(this.first < that.getFirst()){
            return true;
        }else if ( this.first == that.getFirst() && this.second < that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(SecondarySortKey that) {
        if(this.first > that.getFirst()){
            return true;
        }else if (this.first == that.first && this.second > that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortKey that) {
       if(this.$less(that)){
           return true;
       }else if(this.first == that.getFirst() && this.second == that.getSecond()){
           return true;
       }
       return false;
    }

    @Override
    public boolean $greater$eq(SecondarySortKey that) {
        if(this.$greater(that)){
            return true;
        }else if(this.first == that.getFirst() && this.second == that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(SecondarySortKey that) {
        if(this.first != that.first){
            return this.first -that.getFirst();
        }else {
            return this.second - that.getSecond();
        }

    }

    public void setFirst(int first) {
        this.first = first;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SecondarySortKey)) return false;

        SecondarySortKey that = (SecondarySortKey) o;

        if (getFirst() != that.getFirst()) return false;
        return getSecond() == that.getSecond();
    }

    @Override
    public int hashCode() {
        int result = getFirst();
        result = 31 * result + getSecond();
        return result;
    }
}
