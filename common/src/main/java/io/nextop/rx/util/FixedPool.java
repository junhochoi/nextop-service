package io.nextop.rx.util;

import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.observers.Subscribers;
import rx.subscriptions.Subscriptions;

import javax.annotation.Nullable;
import java.util.*;

// FIXME implementing a pool with Rx seems fundamentally wrong
// FIXME because the subscriber may close the subscription before
// FIXME passing the value to the end observable;
// FIXME there is no guarantee that the end observable onNext is called
// FIXME before unsubscribe from the pool.
// FIXME Post this example to the mailing list, and see what people say
// FIXME (brien) I think this pool and DataSourceProvider should go back to the traditional with(closure) interface,
// FIXME which guaranttes the resource is valid during the closure call
public class FixedPool<T> implements AutoCloseable {
    private int maxSize;
    private final Func0<T> source;
    private final Action1<T> sink;
    private final Func1<T, Boolean> verifier;

    private int size;
    private int outSize;
    private int head;
    private SortedMap<Integer, Subscriber<? super T>> pending;
    private Queue<T> values;


    public FixedPool(int maxSize, Func0<T> source, Action1<T> sink, Func1<T, Boolean> verifier) {
        this.maxSize = maxSize;
        this.source = source;
        this.sink = sink;
        this.verifier = verifier;

        size = 0;
        outSize = 0;
        head = 0;
        pending = new TreeMap<>();
        values = new LinkedList<>();
    }


    // important: this blocks until all subscriptions are fulfilled
    @Override
    public void close() {
        synchronized (this) {
            // wait for all pending subscribers and all subscriptions to close
            while (0 < pending.size() || 0 < outSize) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    // ignore
                }
            }

            for (T value; null != (value = values.poll()); ) {
                sink.call(value);
                --size;
            }
            assert 0 == size;

            maxSize = 0;
        }
    }

    private void receive(Subscriber<? super T> subscriber) {
        @Nullable T value;
        synchronized (this) {
            while (null != (value = values.poll()) && !verifier.call(value)) {
                sink.call(value);
                --size;
            }
            if (null == value && size < maxSize) {
                value = source.call();
                assert verifier.call(value);
                ++size;
            }
            if (null == value) {
                int index = head++;
                pending.put(index, subscriber);
                subscriber.add(Subscriptions.create(() -> {
                    synchronized (this) {
                        pending.remove(index);
                    }
                }));
            } else {
                ++outSize;
            }
            notifyAll();
        }
        if (null != value) {
            push(subscriber, value);
        }
    }
    private void push(T value) {
        @Nullable Subscriber subscriber = null;
        synchronized (this) {
            while (!pending.isEmpty()
                    && null != (subscriber = pending.remove(pending.firstKey()))
                    && subscriber.isUnsubscribed()) {
                subscriber = null;
            }
            if (null == subscriber) {
                --outSize;
                if (verifier.call(value)) {
                    values.add(value);
                } else {
                    sink.call(value);
                    --size;
                }
            }
            notifyAll();
        }
        if (null != subscriber) {
            push(subscriber, value);
        }
    }
    private void push(Subscriber<? super T> subscriber, T value) {
        subscriber.add(Subscriptions.create(() -> {
            push(value);
        }));
        subscriber.onNext(value);
        subscriber.onCompleted();
    }


    /** emitted value is valid for the duration between subscription and the first of unsubscribe/onComplete/onError */
    public Observable<T> getSingleObservable() {
        return Observable.create(subscriber -> {
            receive(subscriber);
        });
    }
}
