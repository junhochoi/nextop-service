package io.nextop;

import rx.Observable;
import rx.functions.Action0;

import java.util.Arrays;
import java.util.Collection;

public interface ApiComponent {
    default Observable<ApiStatus> init() {
        return Observable.just(ApiStatus.ok());
    }

    public static class Base implements ApiComponent {
        protected Observable<ApiStatus> init = Observable.<ApiStatus>empty();

        @Override
        public Observable<ApiStatus> init() {
            return init;
        }
    }


    public static Observable<ApiStatus> layerInit(Observable<? extends ApiStatus>... sources) {
        return layerInit(Arrays.asList(sources));
    }

    public static Observable<ApiStatus> layerInit(Collection<? extends Observable<? extends ApiStatus>> sources) {
        return layerInit(sources, () -> {}, () -> {});
    }

    public static Observable<ApiStatus> layerInit(Collection<? extends Observable<? extends ApiStatus>> sources,
                                                   Action0 subscribe, Action0 unsubscribe) {
        // FIXME layer
        return Observable.merge(Observable.from(sources)
        ).doOnSubscribe(subscribe
                // FIXME bring this back
//        ).doOnUnsubscribe(unsubscribe)
        ).share();
    }
}
