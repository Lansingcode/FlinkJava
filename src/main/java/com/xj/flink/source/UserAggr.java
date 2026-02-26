package com.xj.flink.source;

import scala.math.BigInt;

public class UserAggr {
    public String user;
    public Long pv;

    public UserAggr() {}

    public UserAggr(String user, Long pv) {
        this.user = user;
        this.pv = pv;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Long getPv() {
        return pv;
    }

    public void setPv(Long pv) {
        this.pv = pv;
    }
    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", pv='" + pv + '\'' +
                '}';
    }
}