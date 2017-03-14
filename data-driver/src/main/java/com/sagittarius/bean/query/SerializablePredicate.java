package com.sagittarius.bean.query;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * Created by hadoop on 17-3-14.
 */
public interface SerializablePredicate<T> extends Predicate<T>, Serializable {

}
