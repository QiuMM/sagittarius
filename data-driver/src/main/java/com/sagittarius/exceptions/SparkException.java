package com.sagittarius.exceptions;

/**
 * Exception thrown by spark
 */
public class SparkException extends Exception {
    private static final long serialVersionUID = -880650624188364866L;

    public SparkException(String message) {
        super(message);
    }

    public SparkException(String message, Throwable cause) {
        super(message, cause);
    }
}
