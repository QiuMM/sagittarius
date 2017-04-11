package com.sagittarius.exceptions;

/**
 * Exception thrown when a query cannot be performed because no host is
 * available.
 */
public class NoHostAvailableException extends Exception {
    private static final long serialVersionUID = -6910379917446936293L;

    public NoHostAvailableException(String message) {
        super(message);
    }

    public NoHostAvailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
