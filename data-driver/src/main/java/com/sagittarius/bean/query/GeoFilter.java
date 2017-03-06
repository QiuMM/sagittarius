package com.sagittarius.bean.query;

import com.sagittarius.bean.result.GeoPoint;

public class GeoFilter {
    private NumericFilter latitudeFilter;
    private NumericFilter longitudeFilter;

    public GeoFilter setLatitudeFilter(NumericFilter numericFilter){
        this.latitudeFilter = numericFilter;
        return this;
    }

    public GeoFilter setLongitudeFilter(NumericFilter numericFilter){
        this.longitudeFilter = numericFilter;
        return this;
    }

    public boolean toDoOrNotToDo(float g, float l){
        return latitudeFilter.toDoOrNotToDo(g) && longitudeFilter.toDoOrNotToDo(l);
    }
}
