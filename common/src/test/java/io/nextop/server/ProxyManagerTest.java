package io.nextop.server;

import io.nextop.Id;
import io.nextop.Message;
import io.nextop.Route;
import io.nextop.client.MessageContext;
import io.nextop.client.MessageContexts;
import io.nextop.client.MessageControlState;
import io.nextop.wire.WireFactoryPair;
import io.nextop.client.node.Head;
import io.nextop.client.node.nextop.NextopNode;
import io.nextop.rx.MoreSchedulers;
import io.nextop.server.cache.MemoryStorage;
import junit.framework.TestCase;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;

import javax.annotation.Nullable;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class ProxyManagerTest extends TestCase {

    // tests the proxy manager with a local connection
    public void testProxy() throws Exception {
        Scheduler testScheduler = MoreSchedulers.serial();

        // run the test on the correct scheduler
        ProxyTest test = new ProxyTest(testScheduler);
        testScheduler.createWorker().schedule(test);

        test.join();
    }

    static final class ProxyTest implements Action0 {
        final Scheduler scheduler;

        @Nullable
        volatile Exception e = null;
        final Semaphore end = new Semaphore(0);

        int n = 1000;

        final List<Message> send = new LinkedList<Message>();
        final List<Message> receive = new LinkedList<Message>();


        ProxyTest(Scheduler scheduler) {
            this.scheduler = scheduler;
        }


        void join() throws Exception {
            end.acquire();
            if (null != e) {
                throw e;
            }
        }

        void end(@Nullable Exception e) {
            this.e = e;
            end.release();
        }


        @Override
        public void call() {
            try {
                run();
                scheduler.createWorker().schedule(new Action0() {
                    @Override
                    public void call() {
                        try {
                            check();
                        } catch (Exception e) {
                            end(e);
                        }

                        end(null);
                    }
                }, 10, TimeUnit.SECONDS);
            } catch (Exception e) {
                end(e);
            }
        }

        private void run() throws Exception {
            final Id clientId = Id.create();


            WireFactoryPair wfp = new WireFactoryPair();

            NextopNode nextopNode = new NextopNode();
            nextopNode.setWireFactory(wfp.getA());

            MessageContext context = MessageContexts.create();
            MessageControlState mcs = new MessageControlState(context);
            Head head = Head.create(context, mcs, nextopNode, scheduler);


            head.init(null);
            head.onActive(true);


            Executor workExecutor = Executors.newFixedThreadPool(8);
            Cache cache = new Cache(new MemoryStorage(1024 * 1024 * 1024));

            ProxyManager proxyManager = new ProxyManager(workExecutor, cache);


            wfp.observeB().map(wf -> {
                try {
                    return new NextopSession(clientId, Id.create(), wf.create(null));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }).subscribe(proxyManager);



            Subscription a = head.defaultReceive().subscribe(new Action1<Message>() {
                @Override
                public void call(Message message) {
                    receive.add(message);
                }
            });

            for (int i = 0; i < n; ++i) {
                Message message = Message.newBuilder()
                        .setRoute(Route.valueOf("POST http://tests.nextop.io"))
                        .build();
                send.add(message);
                head.send(message);
            }
        }

        void check() throws Exception {
            assertEquals(send.size(), receive.size());
            // FIXME test content
        }
    }


}
