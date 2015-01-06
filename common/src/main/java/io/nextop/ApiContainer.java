package io.nextop;

import org.apache.http.HttpStatus;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.subjects.ReplaySubject;

import javax.annotation.Nullable;
import java.util.logging.Logger;

public final class ApiContainer implements AutoCloseable {
    private static final Logger localLog = Logger.getGlobal();


    final ReplaySubject<ApiStatus> initSubject;
    @Nullable
    Subscription subscription;

    public ApiContainer(ApiComponent component) {
        initSubject = ReplaySubject.create();

        subscription = component.init().flatMap(status -> {
            switch (status.code) {
                case HttpStatus.SC_OK:
                    return Observable.just(status);
                default:
                    return Observable.just(status).concatWith(Observable.error(new ApiException(status)));
            }
        }).subscribe(initSubject);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                close();
            }
        });
    }

    @Override
    public synchronized void close() {
        if (null != subscription) {
            // let the base fully init
            initSubject.toBlocking().last();

            subscription.unsubscribe();
            subscription = null;

            initSubject.onCompleted();
        }
    }

    public Observable<ApiStatus> getInitStatusObservable() {
        return initSubject;
    }
}
