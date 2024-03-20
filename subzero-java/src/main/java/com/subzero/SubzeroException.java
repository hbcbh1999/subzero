package com.subzero;

// custom class to hold the exception message
public class SubzeroException extends Exception {
    private int httpStatusCode;

    public SubzeroException(String message) {
        super(message);
        this.httpStatusCode = 500;
    }

    public SubzeroException(String message, int httpStatusCode) {
        super(message);
        this.httpStatusCode = httpStatusCode;
    }

    public SubzeroException(String message, int httpStatusCode, String description) {
        super("{\"message\":\"" + message + "\",\"description\":\"" + description + "\"}");
        this.httpStatusCode = 500;
    }

    public int getHttpStatusCode() {
        return httpStatusCode;
    }
}
