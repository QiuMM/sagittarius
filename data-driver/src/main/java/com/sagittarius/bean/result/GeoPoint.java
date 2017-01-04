package com.sagittarius.bean.result;

public class GeoPoint extends AbstractPoint {
    private float latitude;
    private float longitude;

    public GeoPoint(String metric, long primaryTime, long secondaryTime, float latitude, float longitude) {
        super(metric, primaryTime, secondaryTime);
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
