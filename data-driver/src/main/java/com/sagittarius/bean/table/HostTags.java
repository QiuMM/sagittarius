package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.util.Map;

/**
 * class map to cassandra table host_tags
 */
@Table(name = "host_tags")
public class HostTags {
    private String host;
    private Map<String, String> tags;

    public HostTags() {
    }

    public HostTags(String host, Map<String, String> tags) {
        this.host = host;
        this.tags = tags;
    }

    @PartitionKey
    @Column(name = "host")
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Column(name = "tags")
    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }
}
