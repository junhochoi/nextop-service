package io.nextop.service.log;

import io.nextop.ApiComponent;
import io.nextop.ApiContainer;
import rx.functions.Func0;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.stream.Stream.*;

public final class ServiceLog extends ApiComponent.Base {
    // FIXME config for remote logging


    private final Level defaultLevel = Level.INFO;
    private final Level handledLevel = Level.WARNING;
    private final Level unhandledLevel = Level.SEVERE;

    // STYLE

    private final int keyWidth = 64;
    private final String keyFormat;

    // FIXME
    private final Logger logger = Logger.getLogger("ServiceLog");


    public ServiceLog() {
        keyFormat = "%-" + keyWidth + "s";
    }


    public void count(String key) {
        count(defaultLevel, key, 1);
    }

    public void count(String key, int d) {
        count(defaultLevel, key, d);
    }

    public void count(Level level, String key, int d) {
        // FIXME
        logger.log(level, String.format(keyFormat + "%+d", key, d));
    }

    public void value(String key, long value) {
        value(defaultLevel, key, value);
    }

    public void value(Level level, String key, long value) {
        // FIXME
        logger.log(level, String.format(keyFormat + "%+d", key, value));
    }

    public <R> R duration(String key, Func0<R> eval) {
        return duration(defaultLevel, key, eval);
    }

    public <R> R duration(Level level, String key, Func0<R> eval) {
        // FIXME
        long time0 = System.nanoTime();
        R r = eval.call();
        long time1 = System.nanoTime();
        duration(level, key, time1 - time0, TimeUnit.NANOSECONDS);
        return r;
    }

    public <R> R durationWithException(String key, Callable<R> eval) throws Exception {
        return durationWithException(defaultLevel, key, eval);
    }

    /** does not log on exception */
    public <R> R durationWithException(Level level, String key, Callable<R> eval) throws Exception {
        // FIXME
        long time0 = System.nanoTime();
        R r = eval.call();
        long time1 = System.nanoTime();
        duration(level, key, time1 - time0, TimeUnit.NANOSECONDS);
        return r;
    }

    public void duration(String key, long time, TimeUnit unit) {
        duration(defaultLevel, key, time, unit);
    }

    public void duration(Level level, String key, long time, TimeUnit unit) {
        // FIXME
        logger.log(level, String.format(keyFormat + "%.3f", key, unit.toMicros(time) / 1000.0));
    }

    public void message(String key) {
        message(defaultLevel, key, "");
    }

    public void message(String key, String messageFormat, Object ... args) {
        message(defaultLevel, key, messageFormat, args);
    }

    public void message(Level level, String key, String messageFormat, Object ... args) {
        // FIXME
        logger.log(level, String.format(keyFormat + messageFormat, concat(of(key), of(args)).toArray()));
    }

    public void trace(String key, String messageFormat, Object ... args) {
        trace(defaultLevel, key, messageFormat, args);
    }

    public void trace(Level level, String key, String messageFormat, Object ... args) {
        // FIXME
        final class Trace extends Exception {}
        logger.log(level, String.format(keyFormat + messageFormat, concat(of(key), of(args)).toArray()),
                new Trace());
    }


    public void handled(String key, Throwable t) {
        handled(handledLevel, key, t);
    }

    public void handled(Level level, String key, Throwable t) {
        // FIXME
        logger.log(level, String.format(keyFormat, key), t);
    }

    public void handled(String key, Throwable t, String messageFormat, Object ... args) {
        handled(handledLevel, key, t, messageFormat, args);
    }

    public void handled(Level level, String key, Throwable t, String messageFormat, Object ... args) {
        // FIXME
        logger.log(level, String.format(keyFormat + messageFormat, concat(of(key), of(args)).toArray()),
                t);
    }


    public void unhandled(String key, Throwable t) {
        unhandled(unhandledLevel, key, t);
    }

    public void unhandled(Level level, String key, Throwable t) {
        // FIXME
        logger.log(level, String.format(keyFormat, key), t);
    }

    public void unhandled(String key, Throwable t, String messageFormat, Object ... args) {
        unhandled(unhandledLevel, key, t, messageFormat, args);
    }

    public void unhandled(Level level, String key, Throwable t, String messageFormat, Object ... args) {
        // FIXME
        logger.log(level, String.format(keyFormat + messageFormat, concat(of(key), of(args)).toArray()),
                t);
    }
}
