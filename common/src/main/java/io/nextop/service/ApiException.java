package io.nextop.service;

import javax.annotation.Nullable;

public final class ApiException extends RuntimeException {
    /////// 2XX ///////

    public static ApiException ok() {
        return new ApiException(ApiStatus.ok());
    }

    public static ApiException created() {
        return new ApiException(ApiStatus.created());
    }

    public static ApiException accepted() {
        return new ApiException(ApiStatus.accepted());
    }

    /////// 4XX ///////

    public static ApiException gone() {
        return new ApiException(ApiStatus.gone());
    }

    public static ApiException unauthorized() {
        return new ApiException(ApiStatus.unauthorized());
    }

    public static ApiException badRequest() {
        return new ApiException(ApiStatus.badRequest());
    }

    /////// 5XX ///////

    public static ApiException internalError() {
        return new ApiException(ApiStatus.internalError());
    }


    public final ApiStatus status;


    public ApiException(Throwable cause) {
        this(ApiStatus.internalError(), cause);
    }

    public ApiException(ApiStatus status) {
        this(status, null);
    }

    public ApiException(ApiStatus status, @Nullable Throwable cause) {
        super(cause);
        this.status = status;
    }
}
