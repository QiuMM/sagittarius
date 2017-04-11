package com.sagittarius.exceptions;

/**
 * A timeout during a query
 */
public class TimeoutException extends Exception {
    private static final long serialVersionUID = -3189707973338390395L;

    public TimeoutException(String message) {
        super(message);
    }

    public TimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
