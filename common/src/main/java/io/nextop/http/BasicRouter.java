package io.nextop.http;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.netty.handler.codec.http.*;
import rx.Observable;
import rx.functions.Func1;

import java.util.*;
import java.util.function.Function;

public class BasicRouter implements Router {

    public static Object var(String parameterName, Func1<String, Object> parser) {
        return new Variable(parameterName, segment -> Collections.singletonList(parser.call(segment)));
    }

    public static Object vars(String parameterName, Func1<String, List<Object>> parser) {
        return new Variable(parameterName, parser);
    }


    private final Multimap<HttpMethod, Route> routes = ArrayListMultimap.create(4, 16);


    public BasicRouter() {

    }


    @Override
    public Observable<HttpResponse> route(HttpMethod method, List<String> segments, Map<String, List<?>> parameters) {
        try {
            for (Route route : routes.get(method)) {
                if (Matcher.next(-1, route.matchers, -1, segments, parameters)) {
                    return route.responseGenerator.apply(parameters);
                }
            }
        } catch (Exception e) {
            return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST));
        }
        // no route
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND));
    }


    /** Add a route.
     * @param segments constant segment strings or {@link #var} */
    public BasicRouter add(HttpMethod method, List<Object> segments,
                           Function<Map<String, List<?>>, Observable<HttpResponse>> responseGenerator) {
        List<Matcher> matchers = new ArrayList<Matcher>(segments.size() + 1);
        for (Object segment : segments) {
            Matcher matcher;
            if (segment instanceof Matcher) {
                matcher = (Matcher) segment;
            } else if (segment instanceof CharSequence) {
                matcher = new Constant(segment.toString());
            } else {
                throw new IllegalArgumentException();
            }
            matchers.add(matcher);
        }
        matchers.add(new Tail());
        routes.put(method, new Route(method, matchers, responseGenerator));
        return this;
    }



    private static final class Route {
        final HttpMethod method;
        final List<Matcher> matchers;
        final Function<Map<String, List<?>>, Observable<HttpResponse>> responseGenerator;

        Route(HttpMethod method, List<Matcher> matchers,
              Function<Map<String, List<?>>, Observable<HttpResponse>> responseGenerator) {
            this.method = method;
            this.matchers = matchers;
            this.responseGenerator = responseGenerator;
        }
    }



    /////// MATCHERS ///////

    private static abstract class Matcher {
        abstract boolean match(int matcherIndex, List<Matcher> matchers, int segmentIndex, List<String> segments, Map<String, List<?>> parameters);

        static boolean next(int matcherIndex, List<Matcher> matchers, int segmentIndex, List<String> segments, Map<String, List<?>> parameters) {
            return matcherIndex + 1 < matchers.size()
                    && matchers.get(matcherIndex + 1).match(matcherIndex + 1, matchers,
                    segmentIndex + 1, segments, parameters);
        }
    }

    private static final class Tail extends Matcher {
        @Override
        public boolean match(int matcherIndex, List<Matcher> matchers, int segmentIndex, List<String> segments, Map<String, List<?>> parameters) {
            return segmentIndex == segments.size();
        }
    }

    private static final class Constant extends Matcher {
        final String segmentValue;

        Constant(String segmentValue) {
            this.segmentValue = segmentValue;
        }

        @Override
        public boolean match(int matcherIndex, List<Matcher> matchers, int segmentIndex, List<String> segments, Map<String, List<?>> parameters) {
            return segmentIndex < segments.size()
                && segmentValue.equals(segments.get(segmentIndex))
                    && next(matcherIndex, matchers, segmentIndex, segments, parameters);
        }
    }

    private static final class Variable extends Matcher {
        final String parameterName;
        final Func1<String, List<Object>> parser;

        Variable(String parameterName, Func1<String, List<Object>> parser) {
            this.parameterName = parameterName;
            this.parser = parser;
        }

        @Override
        boolean match(int matcherIndex, List<Matcher> matchers, int segmentIndex, List<String> segments, Map<String, List<?>> parameters) {
            // defer this match (allow a faster check in the tail to fail)
            if (segmentIndex < segments.size() &&
                    next(matcherIndex, matchers, segmentIndex, segments, parameters)) {
                List<Object> parameter;
                try {
                    parameter = parser.call(segments.get(segmentIndex));
                } catch (Exception e) {
                    return false;
                }
                parameters.put(parameterName, parameter);
                return true;
            } else {
                return false;
            }
        }
    }
}
