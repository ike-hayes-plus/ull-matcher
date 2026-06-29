package io.github.ike.ullmatcher.sdk;

public final class MatcherClientException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final int statusCode;
    private final String responseBody;

    public MatcherClientException(String message, int statusCode, String responseBody) {
        super(message);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public MatcherClientException(String message, Throwable cause) {
        super(message, cause);
        this.statusCode = -1;
        this.responseBody = "";
    }

    public int statusCode() {
        return statusCode;
    }

    public String responseBody() {
        return responseBody;
    }
}
