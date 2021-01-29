package com.github.mgunlogson.cuckoofilter4j;

import java.io.Serializable;
import java.util.Objects;

public class Victim implements Serializable {
    private static final long serialVersionUID = -984233593241086192L;
    private long i1;
    private long i2;
    private long tag;
    /**
     * count 可能的取值为1，>=4,如果为1则直接插入，如>=4则插入tagBox
     * **/
    private int count;

    public Victim(){}

    public Victim(long i1, long i2, long tag, int count) {
        this.i1 = i1;
        this.i2 = i2;
        this.tag = tag;
        this.count = count;
    }

    public Victim(long i1, long i2, long tag) {
        this.i1 = i1;
        this.i2 = i2;
        this.tag = tag;
        this.count = 1;
    }

    public long getI1() {
        return i1;
    }

    public void setI1(long i1) {
        this.i1 = i1;
    }

    public long getI2() {
        return i2;
    }

    public void setI2(long i2) {
        this.i2 = i2;
    }

    public long getTag() {
        return tag;
    }

    public void setTag(long tag) {
        this.tag = tag;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public int hashCode() {
        return Objects.hash(i1, i2, tag, count);
    }

    @Override
    public String toString() {
        return "Victim{" +
                "i1=" + i1 +
                ", i2=" + i2 +
                ", tag=" + tag +
                ", count=" + count +
                '}';
    }
}
