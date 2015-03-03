package io.nextop.server;

import io.nextop.Id;
import io.nextop.client.MessageContext;
import io.nextop.client.MessageContexts;
import io.nextop.client.MessageControlState;
import io.nextop.client.node.Head;
import io.nextop.client.node.http.HttpNode;
import io.nextop.rx.MoreSchedulers;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
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

    final Scheduler scheduler;
    final Head out;


    final Object proxyMutex = new Object();
    // client id -> proxy state
    final Map<Id, ProxyState> proxyStates = new HashMap<Id, ProxyState>(64);



    public ProxyManager(Executor workExecutor, Cache cache) {
        this.workExecutor = workExecutor;
        this.cache = cache;

        scheduler = MoreSchedulers.serial(workExecutor);
        out = createOutHead();

        out.init(null);
    }
    private Head createOutHead() {
        MessageContext context = MessageContexts.create(workExecutor);
        MessageControlState mcs = new MessageControlState(context);

        // FIXME config
        HttpNode.Config outConfig = new HttpNode.Config("Nextop", 8);
        HttpNode outHttp = new HttpNode(outConfig);

        return Head.create(context, mcs, outHttp, scheduler);
    }


    public void start() {
        out.start();
    }

    public void stop() {
        out.stop();
        // FIXME stop all proxies
    }


    @Override
    public void onNext(NextopSession session) {
        ProxyState proxyState;
        synchronized (proxyMutex) {
            proxyState = proxyStates.get(session.clientId);
            if (null == proxyState) {
                Proxy proxy = new Proxy(scheduler, out, session.clientId, workExecutor, cache);
                proxy.start();
                proxyState = new ProxyState(proxy);
            } // TODO else update stats
        }

        System.out.printf("NEXT PROXY SESSION\n");
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
        // FIXME stats for vacuum

        ProxyState(Proxy proxy) {
            this.proxy = proxy;
        }
    }
}
