package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by qmm on 2016/12/15.
 * class map to table owner
 */

@Table(name = "owner",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class Owner {
    private String user;
    private String host;

    public Owner(String user, String host) {
        this.user = user;
        this.host = host;
    }

    public Owner() {

    }

    @PartitionKey
    @Column(name = "user")
    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    @ClusteringColumn
    @Column(name = "host")
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
