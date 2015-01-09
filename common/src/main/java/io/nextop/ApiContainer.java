package io.nextop;

import rx.Observer;
import rx.functions.Action1;

import java.util.logging.Logger;

public final class ApiContainer implements ApiComponent.Init, AutoCloseable {
    private static final Logger localLog = Logger.getGlobal();

    final ApiComponent.Init init;

    public ApiContainer(ApiComponent component) {
        init = component.init();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                stop();
            }
        });
    }

    public boolean start(Action1<ApiStatus> action) {
        return start(
        // TODO interesting error here when using a lambda:
        // multiple non-overriding abstract methods found
        new Observer<ApiStatus>() {
            @Override
            public void onNext(ApiStatus status) {
                action.call(status);
            }
            @Override
            public void onError(Throwable e) {
                throw new ApiException(e);
            }
            @Override
            public void onCompleted() {
                // ignore
            }
        }
        );
    }

    @Override
    public boolean start(Observer<ApiStatus> statusSink) {
        return init.start(statusSink);
    }

    @Override
    public void stop() {
        init.stop();
    }

    @Override
    public void close() throws Exception {
        stop();
    }
}
