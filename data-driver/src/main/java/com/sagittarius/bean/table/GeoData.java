package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * class map to cassandra table data_geo
 */

@Table(name = "data_geo")
public class GeoData extends AbstractData {
    private float latitude;
    private float longitude;

    public GeoData(String host, String metric, String timeSlice, long primaryTime, Long secondaryTime, float latitude, float longitude) {
        super(host, metric, timeSlice, primaryTime, secondaryTime);
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
