package io.nextop.server;

import io.nextop.Message;
import io.nextop.client.Wire;
import io.nextop.client.node.Head;
import io.nextop.client.node.nextop.NextopNode;
import rx.Observer;
import rx.functions.Action1;

import javax.annotation.Nullable;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public final class Proxy implements Observer<Wire> {


    final Head in;
    final NextopNode inNextop;
    final Head out;

    PublishWireFactory wireFactory;

    Cache cache;

    // FIXME process executor

    public Proxy() {
        // set up nodes
        // set wireFactory into inNextop
        // set active


        // FIXME set scheduler on head
    }



    void start() {

        in.defaultReceive().subscribe(new Action1<Message>() {
            @Override
            public void call(Message message) {

            }
        });

    }

    void stop() {

    }


    void proxy(Message request) {


        // - check cache
        // - out.send(message).subscribe(...)
        // - end: in.send(reply, message.inboxRoute())


        proxyProbeCache(request);


    }
    void proxyProbeCache(Message request) {

        Observer<Message> cacheOut = new Observer<Message>() {

            @Override
            public void onNext(Message response) {
                // FIXME send back
            }

            @Override
            public void onCompleted() {
                // FIXME complete to in
            }

            @Override
            public void onError(Throwable e) {
                // miss
                proxyForward(request);
            }

        };

        cache.probe(request, cacheOut);

    }
    void proxyForward(final Message request) {
//        final Observer<Message> cacheAndReturnToSender = new Observer<Message>() {
//            @Override
//            public void onCompleted() {
//
//            }
//
//            @Override
//            public void onError(Throwable e) {
//
//            }
//
//            @Override
//            public void onNext(Message message) {
//
//            }
//        };

        Observer<Message> processRawOut = new Observer<Message>() {
            List<Message> responses;

            @Override
            public void onNext(Message message) {
                proxyProcessRawResponse(request, rawResponse, cacheAndReturnToSender);
            }

            @Override
            public void onCompleted() {
                // FIXME set cache
            }

            @Override
            public void onError(Throwable e) {

            }


        };

        out.send(request);
        out.receive(request.inboxRoute()).subscribe(processRawOut);
    }

    void proxyProcessRawResponse(Message request, Message rawResponse, Observer<Message> responseCallback) {

        // if the request has layer headers,
        // - process into layers, emit each to responseCallback

        // FIXME for encoding/resizing, put on the executor

    }



    static interface Cache {
        void probe(Message request, Observer<Message> callback);
        void add(Message request, Message[] responses);
    }



    /////// Observer ///////

    @Override
    public void onNext(Wire wire) {
        wireFactory.onNext(wire);
    }

    @Override
    public void onCompleted() {
        wireFactory.onCompleted();
        stop();
    }

    @Override
    public void onError(Throwable e) {
        wireFactory.onError(e);
        stop();
    }



    // FIXME defaultSubscriber on the in
    // FIXME route to out

    // FIXME on in -> (request) -> out
    // FIXME intercept and do:
    // FIXME  - memcache

    // FIXME on out -> (response) -> in
    // FIXME intercept response and:
    // FIXME memcache
    // FIXME - if request had image layer pragmas, do downsampling


    static final class PublishWireFactory implements Wire.Factory, Observer<Wire> {

        final BlockingQueue<Wire> wires = new LinkedBlockingQueue<Wire>();


        /////// WireFactory ///////

        @Override
        public Wire create(@Nullable Wire replace) throws InterruptedException, NoSuchElementException {
            return wires.take();
        }

        /////// Observer ///////

        @Override
        public void onNext(Wire wire) {
            boolean s = wires.offer(wire);
            assert s;
        }

        @Override
        public void onCompleted() {
            // Do nothing
        }

        @Override
        public void onError(Throwable e) {
            // Do nothing
        }
    }
}
