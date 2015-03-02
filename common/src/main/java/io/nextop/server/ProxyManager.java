package io.nextop.server;

import io.nextop.Id;
import rx.Observable;
import rx.Observer;
import rx.internal.util.SubscriptionList;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;


// connects session source(s) to proxies
// FIXME vacuum / cleanup policy for proxies
public class ProxyManager implements Observer<NextopSession> {

    final Executor workExecutor;
    final Cache cache;


    final Object proxyMutex = new Object();
    // client id -> proxy state
    final Map<Id, ProxyState> proxyStates = new HashMap<Id, ProxyState>(64);



    public ProxyManager(Executor workExecutor, Cache cache) {
        this.workExecutor = workExecutor;
        this.cache = cache;
    }


    @Override
    public void onNext(NextopSession session) {
        ProxyState proxyState;
        synchronized (proxyMutex) {
            proxyState = proxyStates.get(session.clientId);
            if (null == proxyState) {
                proxyState = new ProxyState(new Proxy(session.clientId, workExecutor, cache),
                        session);
            } else {
                proxyState.mostRecentSession = session;
            }
        }
        proxyState.proxy.onNext(session);
    }

    @Override
    public void onCompleted() {
        // Do nothing
    }

    @Override
    public void onError(Throwable e) {
        // Do nothing
    }


    private static final class ProxyState {
        final Proxy proxy;
        NextopSession mostRecentSession;

        ProxyState(Proxy proxy, NextopSession mostRecentSession) {
            this.proxy = proxy;
            this.mostRecentSession = mostRecentSession;
        }
    }
}
