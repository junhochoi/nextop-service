package io.nextop;

import org.apache.http.HttpStatus;
import rx.Observable;
import rx.Observer;
import rx.functions.Action0;
import rx.functions.Action1;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public interface ApiComponent {
    default Init init() {
        return defaultInit(this);
    }

    public static class Base implements ApiComponent {
        protected Init init = defaultInit(this);

        @Override
        public Init init() {
            return init;
        }
    }


    /////// INIT ///////

    /** ideally these would be Observables, where start and stop are the respective subscribe/unsubscribe callbacks.
     * But in Rx the subscription is valid only between subscribe and onCompleted/onError.
     * Any operator that doesn't close the subscription on onCompleted/onError is working against the system,
     * seems like a conceptual bug. */
    public static interface Init {
        boolean start(Observer<ApiStatus> statusSink);
        void stop();
    }

    static Init defaultInit(Object obj) {
        return init(obj.getClass().getSimpleName(), statusSink -> {}, () -> {});
    }

    public static Init init(String okMessage, Action1<Observer<ApiStatus>> start, Action0 stop) {
        return new Init() {
            int c = 0;
            boolean s = false;

            @Override
            public boolean start(Observer<ApiStatus> statusSink) {
                if (1 == ++c) {
                    boolean _s = true;
                    try {
                        MergeObserver<ApiStatus> mo = new MergeObserver<>(statusSink);
                        start.call(mo);
                        mo.tryThrow();
                        statusSink.onNext(new ApiStatus(HttpStatus.SC_OK, okMessage));
                        statusSink.onCompleted();
                    } catch (Throwable t) {
                        statusSink.onError(t);
                        s = false;
                    }
                    s = _s;
                } else {
                    statusSink.onCompleted();
                }
                return s;
            }

            @Override
            public void stop() {
                if (0 == --c) {
                    stop.call();
                }
            }
        };
    }

    public static Init layerInit(Init ... inits) {
        return layerInit(Arrays.asList(inits));
    }

    public static Init layerInit(List<Init> inits) {
        return new Init() {
            int c = 0;
            int d = 0;
            boolean s = false;

            @Override
            public boolean start(Observer<ApiStatus> statusSink) {
                if (1 == ++c) {
                    int n = inits.size();
                    int i = 0;
                    boolean _s = true;
                    try {
                        for (MergeObserver<ApiStatus> mo = new MergeObserver<>(statusSink); i < n && _s; ++i, mo.reset()) {
                            _s &= inits.get(i).start(mo);
                            mo.tryThrow();
                        }
                        statusSink.onCompleted();
                    } catch (Throwable t) {
                        statusSink.onError(t);
                        _s = false;
                    }
                    d = i;
                    s = _s;
                } else {
                    statusSink.onCompleted();
                }
                return s;
            }

            @Override
            public void stop() {
                if (0 == --c) {
                    int i = d - 1;
                    while (0 <= i) {
                        inits.get(i).stop();
                    }
                }
            }
        };
    }

    /** merge onNext down but filter onCompleted/onError */
    static class MergeObserver<T> implements Observer<T> {
        final Observer<T> out;
        @Nullable
        Throwable e = null;

        MergeObserver(Observer<T> out) {
            this.out = out;
        }


        void tryThrow() throws Throwable {
            if (null != e) {
                throw e;
            }
        }

        void reset() {
            e = null;
        }


        @Override
        public void onNext(T t) {
            out.onNext(t);
        }

        @Override
        public void onError(Throwable e) {
            this.e = e;
        }

        @Override
        public void onCompleted() {
            // ignore
        }
    }

}
