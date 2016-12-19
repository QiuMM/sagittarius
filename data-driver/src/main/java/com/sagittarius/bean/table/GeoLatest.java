package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by qmm on 2016/12/18.
 * class map to table latest_geo
 */

@Table(name = "latest_geo",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class GeoLatest extends AbstractLatest {
    private float latitude;
    private float longitude;

    public GeoLatest(String host, String metric, long createdAt, long receivedAt, float latitude, float longitude) {
        super(host, metric, createdAt, receivedAt);
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public GeoLatest() {
    }

    @Column(name = "latitude")
    public float getLatitude() {
        return latitude;
    }

    public void setLatitude(float latitude) {
        this.latitude = latitude;
    }

    @Column(name = "longitude")
    public float getLongitude() {
        return longitude;
    }

    public void setLongitude(float longitude) {
        this.longitude = longitude;
    }
}
