package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by qmm on 2016/12/15.
 * class map to table data_geo
 */

@Table(name = "data_geo",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class GeoData extends AbstractData {
    private float latitude;
    private float longitude;

    public GeoData(String host, String metric, String date, long createdAt, long receivedAt, float latitude, float longitude) {
        super(host, metric, date, createdAt, receivedAt);
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public GeoData() {
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
