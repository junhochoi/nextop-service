package io.nextop.rx;

import rx.Notification;
import rx.Observable;
import rx.Observer;
import rx.functions.Action1;
import rx.observers.SerializedObserver;

import java.util.concurrent.Semaphore;

public class MoreRxOperations {


    public static <T> void blockingSubscribe(Observable<T> source, Action1<T> action) {
        blockingSubscribe(source,
                new Observer<T>() {
                    @Override
                    public void onNext(T t) {
                        action.call(t);
                    }

                    @Override
                    public void onError(Throwable e) {
                        // ignore
                    }

                    @Override
                    public void onCompleted() {
                        // ignore
                    }
                }
        );

    }
    public static <T> void blockingSubscribe(Observable<T> source, Observer<? super T> sink) {
        Semaphore s = new Semaphore(0);

        Observer<? super T> serSink = new SerializedObserver<>(sink);
        source.subscribe(
            (T t) -> {
                serSink.onNext(t);
            },
            (Throwable e) -> {
                try {
                    serSink.onError(e);
                } finally {
                    s.release();
                }
            },
            () -> {
                try {
                    serSink.onCompleted();
                } finally {
                    s.release();
                }
            }
        );

        s.acquireUninterruptibly();
    }
}
