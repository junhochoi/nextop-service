package io.nextop.server;

import io.nextop.client.Wire;
import io.nextop.client.node.Head;
import io.nextop.client.node.nextop.NextopNode;
import rx.Observer;

import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public final class Proxy implements Observer<Wire> {


    final Head in;
    final NextopNode inNextop;
    final Head out;

    PublishWireFactory wireFactory;


    public Proxy() {
        // set up nodes
        // set wireFactory into inNextop
        // set active
    }



    void start() {

//        in.defaultReceive().

    }

    void stop() {

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
