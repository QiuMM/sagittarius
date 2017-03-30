package com.sagittarius.bean.query;

import java.io.Serializable;
import java.util.function.Predicate;


public interface Filter<T> extends Predicate<T>, Serializable {
}
