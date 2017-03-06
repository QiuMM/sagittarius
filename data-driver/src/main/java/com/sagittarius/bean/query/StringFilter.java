package com.sagittarius.bean.query;

public class StringFilter {
    private String regex;

    public StringFilter setRegex(String regex){
        this.regex = regex;
        return this;
    }

    public boolean toDoOrNotToDo(String dataValue){
        if(regex == null){
            return true;
        }
        return dataValue.matches(regex);
    }
}
