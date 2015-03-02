package io.nextop.server.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import io.nextop.server.Cache;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;

import javax.annotation.Nullable;

public class MemoryStorage implements Cache.Storage {

    final com.google.common.cache.Cache<String, byte[]> cache;


    public MemoryStorage(long maxBytes) {
        cache = CacheBuilder.<String, byte[]>newBuilder()
                .maximumWeight(maxBytes)
                .weigher(new Weigher<String, byte[]>() {
                    @Override
                    public int weigh(String key, byte[] value) {
                        return value.length;
                    }
                })
                .build();

    }

    @Override
    public void put(String key, byte[] bytes) {
        cache.put(key, bytes);
    }

    @Override
    public Observable<byte[]> get(final String key) {
        return Observable.create(new Observable.OnSubscribe<byte[]>() {
            @Override
            public void call(Subscriber<? super byte[]> subscriber) {
                @Nullable byte[] bytes = cache.getIfPresent(key);
                if (null != bytes) {
                    subscriber.onNext(bytes);
                    subscriber.onCompleted();
                } else {
                    subscriber.onError(null);
                }
            }
        });
    }
}
