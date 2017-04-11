package com.sagittarius.exceptions;

/**
 * Exception related to the execution of a query (Include read query and write query).
 */
public class QueryExecutionException extends Exception {
    private static final long serialVersionUID = -1642617717093395066L;

    public QueryExecutionException(String message) {
        super(message);
    }

    public QueryExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
