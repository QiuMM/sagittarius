package com.sagittarius.bean.query;

import com.datastax.spark.connector.util.CqlWhereParser;

public class NumericFilter {
    private NumericFilterType numericFilterType;
    private int intValue;
    private long longValue;
    private float floatValue;
    private double doubleValue;



    public NumericFilter setNumericFilterType(NumericFilterType nft){
        this.numericFilterType = nft;
        return this;
    }

    public NumericFilter setNumericFilterValue(int intValue){
        this.intValue = intValue;
        return this;
    }

    public NumericFilter setNumericFilterValue(long longValue){
        this.longValue = longValue;
        return this;
    }

    public NumericFilter setNumericFilterValue(float floatValue){
        this.floatValue = floatValue;
        return this;
    }

    public NumericFilter setNumericFilterValue(double doubleValue){
        this.doubleValue = doubleValue;
        return this;
    }

    public boolean toDoOrNotToDo(int dataValue){
        if(numericFilterType == null){
            return true;
        }

        switch (numericFilterType){
            case EqultTo:
                return dataValue == intValue;
            case LessThan:
                return dataValue < intValue;
            case GreaterThan:
                return dataValue > intValue;
            case LessThanOrEqualTo:
                return dataValue <= intValue;
            case GreaterThanOrEqualTo:
                return dataValue >= intValue;
        }

        return true;
    }

    public boolean toDoOrNotToDo(long dataValue){
        if(numericFilterType == null){
            return true;
        }

        switch (numericFilterType){
            case EqultTo:
                return dataValue == longValue;
            case LessThan:
                return dataValue < longValue;
            case GreaterThan:
                return dataValue > longValue;
            case LessThanOrEqualTo:
                return dataValue <= longValue;
            case GreaterThanOrEqualTo:
                return dataValue >= longValue;
        }

        return true;
    }

    public boolean toDoOrNotToDo(double dataValue){
        if(numericFilterType == null){
            return true;
        }

        switch (numericFilterType){
            case EqultTo:
                return dataValue == floatValue;
            case LessThan:
                return dataValue < floatValue;
            case GreaterThan:
                return dataValue > floatValue;
            case LessThanOrEqualTo:
                return dataValue <= floatValue;
            case GreaterThanOrEqualTo:
                return dataValue >= floatValue;
        }

        return true;
    }

    public boolean toDoOrNotToDo(float dataValue){
        if(numericFilterType == null){
            return true;
        }

        switch (numericFilterType){
            case EqultTo:
                return dataValue == doubleValue;
            case LessThan:
                return dataValue < doubleValue;
            case GreaterThan:
                return dataValue > doubleValue;
            case LessThanOrEqualTo:
                return dataValue <= doubleValue;
            case GreaterThanOrEqualTo:
                return dataValue >= doubleValue;
        }

        return true;
    }

}
