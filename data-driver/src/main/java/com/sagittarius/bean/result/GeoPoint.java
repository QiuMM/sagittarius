package com.sagittarius.bean.result;

/**
 * Created by qmm on 2016/12/17.
 */
public class GeoPoint extends AbstractPoint {
    private float latitude;
    private float longitude;

    public GeoPoint(String metric, long time, float latitude, float longitude) {
        super(metric, time);
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public float getLatitude() {
        return latitude;
    }

    public void setLatitude(float latitude) {
        this.latitude = latitude;
    }

    public float getLongitude() {
        return longitude;
    }

    public void setLongitude(float longitude) {
        this.longitude = longitude;
    }
}
