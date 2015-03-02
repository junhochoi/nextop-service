package io.nextop.server;

import io.nextop.EncodedImage;
import io.nextop.Id;
import io.nextop.Message;
import io.nextop.WireValue;
import io.nextop.client.MessageContext;
import io.nextop.client.MessageContexts;
import io.nextop.client.MessageControlState;
import io.nextop.client.Wire;
import io.nextop.client.node.Head;
import io.nextop.client.node.http.HttpNode;
import io.nextop.client.node.nextop.NextopNode;
import io.nextop.rx.MoreSchedulers;
import io.nextop.util.NoCopyByteArrayOutputStream;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action1;
import rx.subjects.ReplaySubject;

import javax.annotation.Nullable;
import javax.imageio.ImageIO;
import javax.imageio.stream.MemoryCacheImageInputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

// each proxy is bound to a single client ID that may span multiple sessions
public final class Proxy implements Observer<NextopSession> {
    final Id clientId;
    final Executor workExecutor;
    final Cache cache;

    // serial
    final Scheduler scheduler;

    final Head in;
    final Head out;


    // FIXME consider evisting queue or bounded queue
    final BlockingQueue<NextopSession> sessions = new LinkedBlockingQueue<NextopSession>();


    @Nullable
    Subscription inSubscription = null;


    public Proxy(Id clientId, Executor workExecutor, Cache cache) {
        this.clientId = clientId;
        this.workExecutor = workExecutor;
        this.cache = cache;

        scheduler = MoreSchedulers.serial(workExecutor);

        in = createInHead();
        out = createOutHead();
    }
    private Head createInHead() {
        class SessionWireFactory implements Wire.Factory {
            @Override
            public Wire create(@Nullable Wire replace) throws InterruptedException, NoSuchElementException {
                NextopSession session = sessions.take();
                return session.wire;
            }
        }

        MessageContext context = MessageContexts.create(workExecutor);
        MessageControlState mcs = new MessageControlState(context);

        // FIXME config
        NextopNode.Config inConfig = new NextopNode.Config();
        NextopNode inNextop = new NextopNode(inConfig);
        // this is connected via #onSessionCreated
        inNextop.setWireFactory(new SessionWireFactory());

        return Head.create(context, mcs, inNextop, scheduler);
    }
    private Head createOutHead() {
        MessageContext context = MessageContexts.create(workExecutor);
        MessageControlState mcs = new MessageControlState(context);

        // FIXME config
        HttpNode.Config outConfig = new HttpNode.Config("Nextop", 8);
        HttpNode outHttp = new HttpNode(outConfig);

        return Head.create(context, mcs, outHttp, scheduler);
    }



    /////// Observable ///////

    /** thread-safe */
    @Override
    public void onNext(NextopSession session) {
        try {
            sessions.put(session);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /** thread-safe */
    @Override
    public void onCompleted() {
        // Do nothing
    }

    /** thread-safe */
    @Override
    public void onError(Throwable e) {
        // Do nothing
    }






    public void start() {
        if (null == inSubscription) {
            inSubscription = in.defaultReceive().subscribe(new Action1<Message>() {
                @Override
                public void call(Message message) {
                    proxy(message);
                }
            });
        }
    }

    public void stop() {
        if (null != inSubscription) {
            inSubscription.unsubscribe();
            inSubscription = null;
        }
    }



    /////// PROXY PIPELINE ///////

    void proxy(Message request) {
        proxyProbeCache(request);
    }

    void proxyProbeCache(final Message request) {
        // FIXME track this subscription, attach to the request
        Subscription s = cache.probe(clientId, request).subscribe(new Observer<Message>() {
            @Override
            public void onNext(final Message response) {
                // send back
                proxyNextToSender(request, response);
            }

            @Override
            public void onCompleted() {
                proxyCompleteToSender(request);
            }

            @Override
            public void onError(Throwable e) {
                // miss
                proxyForward(request);
            }
        });
    }
    void proxyForward(final Message request) {
        out.send(request);
        // FIXME track this subscription, attach to the request
        Subscription s = out.receive(request.inboxRoute()).subscribe(new Observer<Message>() {
            Observable<Message> replayResponses = Observable.empty();

            @Override
            public void onNext(Message rawResponse) {
                ReplaySubject<Message> subject = ReplaySubject.create();
                replayResponses = replayResponses.concatWith(subject);

                // immediately forward
                subject.observeOn(scheduler).subscribe(response -> proxyNextToSender(request, response));

                proxyProcessRawResponse(request, rawResponse, subject);
            }

            @Override
            public void onCompleted() {
                replayResponses.doOnCompleted(() -> {
                    // proxy complete when all requests are done
                    proxyCompleteToSender(request);
                }).subscribe(new Observer<Message>() {
                    // add to cache
                    final List<Message> responses = new ArrayList<Message>(4);

                    @Override
                    public void onNext(Message message) {
                        responses.add(message);
                    }

                    @Override
                    public void onCompleted() {
                        cache.add(clientId, request,
                                responses.toArray(new Message[responses.size()]));
                    }

                    @Override
                    public void onError(Throwable e) {
                        // FIXME
                    }
                });
            }

            @Override
            public void onError(Throwable e) {
                proxyErrorToSender(request);
            }
        });
    }

    /** @param responseCallback must be thread-safe. It will be called on one executor thread. */
    void proxyProcessRawResponse(Message request, Message rawResponse, Observer<Message> responseCallback) {
        // process the raw response differently depending on type type

        @Nullable WireValue contentValue = rawResponse.getContent();
        if (null == contentValue) {
            responseCallback.onNext(rawResponse);
            responseCallback.onCompleted();
        } else {
            switch (contentValue.getType()) {
                case IMAGE: {
                    @Nullable Message.LayerInfo[] layers = Message.getLayers(request);
                    if (null == layers) {
                        responseCallback.onNext(rawResponse);
                        responseCallback.onCompleted();
                    } else {
                        EncodedImage image = contentValue.asImage();
                        // - decode
                        // - encode+
                        workExecutor.execute(new CreateLayersWorker(request, rawResponse, image, layers, responseCallback));
                    }
                    break;
                }

                default:
                    responseCallback.onNext(rawResponse);
                    responseCallback.onCompleted();
                    break;
            }
        }
    }


    void proxyNextToSender(Message request, Message response) {
        assert response.route.equals(request.inboxRoute());
        in.send(response);
    }
    void proxyCompleteToSender(Message request) {
        in.complete(Message.newBuilder().setRoute(request.inboxRoute()).build());

    }
    void proxyErrorToSender(Message request) {
        in.error(Message.newBuilder().setRoute(request.inboxRoute()).build());
    }


    /////// PROXY PIPELINE: IMAGE LAYERS ///////

    final class CreateLayersWorker implements Runnable {
        final Message request;
        final Message rawResponse;
        final EncodedImage image;
        final Message.LayerInfo[] layers;

        final Observer<Message> responseCallback;

        final int n;

        // output
        final Object outMutex = new Object();
        Message[] responses;
        int responseHead = 0;


        boolean ended = false;


        CreateLayersWorker(Message request, Message rawResponse,
                           EncodedImage imageContent, Message.LayerInfo[] layers, Observer<Message> responseCallback) {
            if (layers.length <= 0) {
                throw new IllegalArgumentException();
            }
            n = layers.length;

            this.request = request;
            this.rawResponse = rawResponse;

            this.image = imageContent;
            this.layers = layers;
            this.responseCallback = responseCallback;

            responses = new Message[n];
        }


        void error(Throwable error) {
            synchronized (outMutex) {
                if (ended) {
                    return;
                }

                ended = true;
            }

            responseCallback.onError(error);
        }

        void emit(int layerIndex, Message response) {
            int start, end;
            synchronized (outMutex) {
                if (ended) {
                    return;
                }

                responses[layerIndex] = response;

                if (layerIndex == responseHead) {
                    start = layerIndex;
                    responseHead += 1;
                    while (responseHead < n && null != responses[responseHead]) {
                        responseHead += 1;
                    }
                    end = responseHead;
                } else {
                    start = -1;
                    end = -1;
                }

                if (n == end) {
                    ended = true;
                }
            }

            if (0 <= start) {
                for (int i = start; i < end; ++i) {
                    responseCallback.onNext(responses[i]);
                }

                if (n == end) {
                    responseCallback.onCompleted();
                }
            }
        }


        @Override
        public void run() {
            // FIXME need faster codecs

            // 1. decode the image
            // 2. launch parallel tasks to encode to the target layer
            // 3. emit the results in order of layers

            BufferedImage memImage;
            try {
                memImage = ImageIO.read(new MemoryCacheImageInputStream(image.getInputStream()));
            } catch (IOException e) {
                // FIXME this should not happen
                error(e);
                return;
            }

            for (int i = 1; i < n; ++i) {
                if (layers[i].isScale()) {
                    workExecutor.execute(new ScaleWorker(i, memImage));
                } else {
                    emit(i, rawResponse);
                }
            }

            // run the first inline
            if (layers[0].isScale()) {
                new ScaleWorker(0, memImage).run();
            } else {
                emit(0, rawResponse);
            }
        }


        final class ScaleWorker implements Runnable {
            int layerIndex;
            BufferedImage memImage;

            ScaleWorker(int layerIndex, BufferedImage memImage) {
                this.layerIndex = layerIndex;
                this.memImage = memImage;
            }

            @Override
            public void run() {
                final Message.LayerInfo layer = layers[layerIndex];

                Map<RenderingHints.Key, Object> hints = new HashMap<RenderingHints.Key, Object>(4);
                switch (layer.quality) {
                    case HIGH:
                        hints.put(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC);
                        hints.put(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
                        break;
                    case LOW:
                        hints.put(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
                        hints.put(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_SPEED);
                        break;
                    default:
                        // no hints
                        break;
                }

                int w = memImage.getWidth();
                int h = memImage.getHeight();
                int sw = layer.scaleWidth(w, h);
                int sh = layer.scaleHeight(w, h);

                BufferedImage scaledMemImage = scale(memImage, sw, sh, hints);

                NoCopyByteArrayOutputStream baos = new NoCopyByteArrayOutputStream(128 * 1024);
                try {
                    MemoryCacheImageOutputStream ios = new MemoryCacheImageOutputStream(baos);
                    try {
                        ImageIO.write(scaledMemImage, layer.format.toString().toLowerCase(), ios);
                    } finally {
                        ios.close();
                    }
                } catch (IOException e) {
                    // FIXME this should not happen
                    error(e);
                    return;
                }

                EncodedImage scaledImage = EncodedImage.create(layer.format, EncodedImage.DEFAULT_ORIENTATION, sw, sh,
                        baos.getBytes(), baos.getOffset(), baos.getLength());

                Message.Builder builder = rawResponse.toBuilder()
                        .setRoute(request.inboxRoute())
                        .setContent(scaledImage);
                if (null != layer.groupId) {
                    builder.setGroupId(layer.groupId)
                        .setGroupPriority(layer.groupPriority);
                }
                Message.setLayers(builder, layer);
                Message response = builder.build();

                emit(layerIndex, response);
            }
        }
    }

    static BufferedImage scale(BufferedImage bi, int sw, int sh, Map<RenderingHints.Key, Object> hints) {
        BufferedImage sbi = new BufferedImage(sw, sh, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2d = sbi.createGraphics();
            // apply hints
            for (Map.Entry<RenderingHints.Key, Object> e : hints.entrySet()) {
                g2d.setRenderingHint(e.getKey(), e.getValue());
            }
            g2d.drawImage(bi, 0, 0, sw, sh, null);
        g2d.dispose();
        return sbi;
    }
}
