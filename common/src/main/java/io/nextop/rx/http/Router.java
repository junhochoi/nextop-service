package io.nextop.rx.http;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import rx.Observable;

import java.util.List;
import java.util.Map;

public interface Router {
    Observable<HttpResponse> route(HttpMethod method, List<String> segments, Map<String, List<?>> parameters);
}
