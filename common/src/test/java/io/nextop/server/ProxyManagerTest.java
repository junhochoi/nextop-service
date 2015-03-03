package io.nextop.server;

import io.nextop.EncodedImage;
import io.nextop.Id;
import io.nextop.Message;
import io.nextop.Route;
import io.nextop.client.MessageContext;
import io.nextop.client.MessageContexts;
import io.nextop.client.MessageControlState;
import io.nextop.client.node.Head;
import io.nextop.client.node.nextop.NextopNode;
import io.nextop.client.test.WorkloadRunner;
import io.nextop.rx.MoreSchedulers;
import io.nextop.server.cache.MemoryStorage;
import io.nextop.wire.Pipe;
import junit.framework.TestCase;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProxyManagerTest extends TestCase {

    // tests the proxy manager with a local connection
    public void testProxy() throws Exception {
        Scheduler testScheduler = MoreSchedulers.serial();

        // run the test on the correct scheduler
        ProxyTest test = new ProxyTest(testScheduler);
        test.start();
        test.join();
    }

    static final class ProxyTest extends WorkloadRunner {
        int n = 200;

        final List<Message> send = new LinkedList<Message>();
        final List<Message> receive = new LinkedList<Message>();


        ProxyTest(Scheduler scheduler) {
            super(scheduler);
        }


        @Override
        public void run() throws Exception {
            final Id clientId = Id.create();


            Pipe wfp = new Pipe();

            NextopNode nextopNode = new NextopNode();
            nextopNode.setWireFactory(wfp.getA());

            MessageContext context = MessageContexts.create();
            MessageControlState mcs = new MessageControlState(context);
            Head head = Head.create(context, mcs, nextopNode, scheduler);


            head.init(null);
            head.start();


            Executor workExecutor = Executors.newFixedThreadPool(8);
            Cache cache = new Cache(new MemoryStorage(1024 * 1024 * 1024));

            ProxyManager proxyManager = new ProxyManager(workExecutor, cache);
            proxyManager.start();


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

        @Override
        public void check() throws Exception {
            assertEquals(send.size(), receive.size());
            // FIXME test content
        }
    }


    public void testImageProxy() throws Exception {
        Scheduler testScheduler = MoreSchedulers.serial();

        // run the test on the correct scheduler
        ImageProxyTest test = new ImageProxyTest(testScheduler);
        test.start();

        test.join();
    }


    static final class ImageProxyTest extends WorkloadRunner {
        int n = 500;

        final List<Message> send = new LinkedList<Message>();
        final List<Message> receive = new LinkedList<Message>();


        ImageProxyTest(Scheduler scheduler) {
            super(scheduler);
        }


        @Override
        protected void run() throws Exception {
            final Id clientId = Id.create();


            Pipe wfp = new Pipe();

            NextopNode nextopNode = new NextopNode();
            nextopNode.setWireFactory(wfp.getA());

            MessageContext context = MessageContexts.create();
            MessageControlState mcs = new MessageControlState(context);
            Head head = Head.create(context, mcs, nextopNode, scheduler);


            head.init(null);
            head.start();


            Executor workExecutor = Executors.newFixedThreadPool(8);
            Cache cache = new Cache(new MemoryStorage(1024 * 1024 * 1024));

            ProxyManager proxyManager = new ProxyManager(workExecutor, cache);
            proxyManager.start();


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


            final Id lowPriorityGroupId = Id.create();
            final Id highPriorityGroupId = Id.create();

            Action0 sendOne = new Action0() {
                @Override
                public void call() {
                    Message.Builder builder  = Message.newBuilder()
                            .setRoute(Route.valueOf("GET http://s3-us-west-2.amazonaws.com/nextop-demo-flip-frames/b5bacea252864f938d851be98fdb1a3900af0ad183bf63b9a9bb321f2e063596-5090de8538ea489c94dc362f20c0cc67ea98dfc67437a990b57ab4ff7ee005d1.jpeg"));

                    Message.setLayers(builder,
                            new Message.LayerInfo(Message.LayerInfo.Quality.LOW, EncodedImage.Format.JPEG, 32, 0,
                                    highPriorityGroupId, 10),
                            new Message.LayerInfo(Message.LayerInfo.Quality.HIGH, EncodedImage.Format.JPEG, 0, 0,
                                    lowPriorityGroupId, 0)
                    );

                    Message message = builder.build();

                    send.add(message);
                    head.send(message);
                }
            };


            // send one then give the cache time to fill
            // TODO won't have to do this with the in-flight module in place!
            sendOne.call();
            for (int i = 1; i < n; ++i) {
                scheduler.createWorker().schedule(sendOne, 4000, TimeUnit.MILLISECONDS);
            }
        }

        @Override
        protected void check() throws Exception {
            assertEquals(2 * send.size(), receive.size());
            // FIXME test content
        }
    }


}
