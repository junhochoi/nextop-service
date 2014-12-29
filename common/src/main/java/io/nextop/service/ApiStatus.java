package io.nextop.service;

import org.apache.http.HttpStatus;

import javax.annotation.Nullable;

public final class ApiStatus {
    /////// 2XX ///////

    public static ApiStatus ok() {
        return new ApiStatus(HttpStatus.SC_OK);
    }

    public static ApiStatus created() {
        return new ApiStatus(HttpStatus.SC_CREATED);
    }

    public static ApiStatus accepted() {
        return new ApiStatus(HttpStatus.SC_ACCEPTED);
    }

    /////// 4XX ///////

    public static ApiStatus gone() {
        return new ApiStatus(HttpStatus.SC_GONE);
    }

    public static ApiStatus unauthorized() {
        return new ApiStatus(HttpStatus.SC_UNAUTHORIZED);
    }

    public static ApiStatus badRequest() {
        return new ApiStatus(HttpStatus.SC_BAD_REQUEST);
    }

    /////// 5XX ///////

    public static ApiStatus internalError() {
        return new ApiStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    }


    /** HTTP status code.
     * @see http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html */
    public final int code;
    @Nullable
    public final String reason;

    public ApiStatus(int code) {
        this(code, null);
    }
    public ApiStatus(int code, @Nullable String reason) {
        this.code = code;
        this.reason = reason;
    }
}
