package io.nextop.rx.util;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.subjects.BehaviorSubject;

import javax.annotation.Nullable;
import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class ConfigWatcher implements AutoCloseable {
    private final Scheduler scheduler;
    private final File[] files;

    private final int intervalMs = 1000;

    private final BehaviorSubject<JsonObject> mergedSubject;
    private final Observable<JsonObject> mergedObservable;


    // INTERNAL SUBSCRIPTIONS

    @Nullable
    private Subscription pollSubscription = null;


    public ConfigWatcher(Scheduler scheduler, String ... fileNames) {
        this(scheduler, (File[]) Arrays.stream(fileNames).map(fileName -> new File(fileName)).toArray(n -> new File[n]));
    }

    public ConfigWatcher(Scheduler scheduler, File ... files) {
        this.scheduler = scheduler;
        this.files = files;

        mergedSubject = BehaviorSubject.create();
        mergedObservable = mergedSubject.doOnSubscribe(() -> {
            pollSubscription = scheduler.createWorker().schedulePeriodically(new Action0() {
                final int n = files.length;
                final long[] lastModifiedTimes = new long[n];
                final JsonObject[] lastConfigObjects = new JsonObject[n];

                @Override
                public void call() {
                    boolean modified = false;

                    for (int i = 0; i < n; ++i) {
                        File f = files[i];
                        if (f.exists()) {
                            long lastModifiedTime = f.lastModified();
                            if (lastModifiedTime != lastModifiedTimes[i]) {
                                try {
                                    Reader r = new BufferedReader(new InputStreamReader(new FileInputStream(f), Charsets.UTF_8));
                                    try {
                                        lastConfigObjects[i] = new JsonParser().parse(r).getAsJsonObject();
                                    } finally {
                                        r.close();
                                    }
                                    lastModifiedTimes[i] = lastModifiedTime;
                                    modified = true;
                                } catch (IOException e) {
                                    // FIXME log
                                    // lave this index untouched; try again next interval
                                }
                            }

                            if (modified) {
                                JsonObject mergedObject = new JsonObject();
                                mergeDown(mergedObject, lastConfigObjects);
                                mergedSubject.onNext(mergedObject);
                            }
                        }
                    }
                }
            }, 0L, intervalMs, TimeUnit.MILLISECONDS);
        }).doOnUnsubscribe(() -> {
            pollSubscription.unsubscribe();
        }).share();
    }

    @Override
    public void close() throws Exception {
        pollSubscription.unsubscribe();
    }


    public Observable<JsonObject> getMergedObservable() {
        return mergedSubject;
    }


    /** @param objects higher indexes have precedence */
    private static void mergeDown(JsonObject mergedObject, JsonObject ... objects) {
        for (int i = objects.length - 1; 0 <= i; --i) {
            JsonObject object = objects[i];
            for (Map.Entry<String, JsonElement> e : object.entrySet()) {
                String key = e.getKey();
                JsonElement value = e.getValue();
                if (!mergedObject.has(key)) {
                    mergedObject.add(key, value);
                } else if (mergedObject.get(key).isJsonObject() && value.isJsonObject()) {
                    mergeDown(mergedObject.get(key).getAsJsonObject(), value.getAsJsonObject());
                } // else the value in the merged object takes precedence
            }
        }
    }
}
