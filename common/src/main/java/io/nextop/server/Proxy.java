package io.nextop.server;

import io.nextop.*;
import io.nextop.client.MessageContext;
import io.nextop.client.MessageContexts;
import io.nextop.client.MessageControlState;
import io.nextop.client.node.Head;
import io.nextop.client.node.nextop.NextopNode;
import io.nextop.log.LogEntry;
import io.nextop.log.NL;
import io.nextop.util.NoCopyByteArrayOutputStream;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action1;
import rx.subjects.ReplaySubject;

import javax.annotation.Nullable;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageInputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
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


    public Proxy(Scheduler scheduler, Head out, Id clientId, Executor workExecutor, Cache cache) {
        this.scheduler = scheduler;
        this.out = out;

        this.clientId = clientId;
        this.workExecutor = workExecutor;
        this.cache = cache;

        in = createInHead();

        in.init(null);
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
        NextopNode inNextop = new NextopNode();
        // this is connected via #onSessionCreated
        inNextop.setWireFactory(new SessionWireFactory());

        return Head.create(context, mcs, inNextop, scheduler);
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
            NL.nl.count("proxy.start");
            in.start();

            inSubscription = in.defaultReceive().subscribe(new Action1<Message>() {
                @Override
                public void call(Message message) {
                    try {
                        proxy(message);
                    } catch (Throwable t) {
                        // FIXME log
                        t.printStackTrace();
                    }
                }
            });
        }
    }

    public void stop() {
        if (null != inSubscription) {
            NL.nl.count("proxy.stop");
            inSubscription.unsubscribe();
            inSubscription = null;

            in.stop();
        }
    }


    /////// PROXY PIPELINE ///////

    void proxy(Message request) {
        NL.nl.message("proxy.request", "Start proxy for %s", request);
        NL.nl.count("proxy.request");

        if (Message.isLocal(request.route)) {
            // session control
            if (Message.logRoute().equals(request.route)) {
                try {
                    LogEntry entry = LogEntry.fromWireValue(request.getContent());
                    entry.writeTo(NL.nl);
                } catch (Throwable t) {
                    NL.nl.unhandled("proxy.request.control.log", t);
                }
            } else {
                NL.nl.message("proxy.request.control", "Unknown local route %s", request.route);
            }
        } else {
            // FIXME
//        proxyProbeCache(request);
            proxyForward(request);
        }
    }

    void proxyProbeCache(final Message request) {
        // FIXME track this subscription, attach to the request
        Subscription s = cache.probe(clientId, request).subscribe(new Observer<Message>() {
            @Override
            public void onNext(final Message response) {
                // send back
                NL.nl.message("proxy.request", "Cache hit for %s -> %s", request, response);
                proxyNextToSender(request, response);
            }

            @Override
            public void onCompleted() {
                NL.nl.message("proxy.request", "Cache complete for %s", request);
                proxyCompleteToSender(request);
            }

            @Override
            public void onError(Throwable e) {
                NL.nl.message("proxy.request", "Cache miss for %s", request);
                // miss
                proxyForward(request);
            }
        });
    }
    void proxyForward(final Message request) {
        // FIXME(INFLIGHT) same module is needed in the client, for normal and layer (image decode/encode, resize) loads
        // FIXME need an in flight manager here. Attach to an in-flight request if in-flight
        // FIXME receive the processed results of the in-flight request

        NL.nl.message("proxy.request.forward", "Forward %s\n", request);

        out.send(request);
        // FIXME track this subscription, attach to the request
        Subscription s = out.receive(request.inboxRoute()).subscribe(new Observer<Message>() {
//            Observable<Message> replayResponses = Observable.empty();

            @Override
            public void onNext(Message rawResponse) {
//                System.out.printf("Receive raw response for %s -> %s\n", request, rawResponse);
//
//                ReplaySubject<Message> subject = ReplaySubject.create();
//                replayResponses = replayResponses.concatWith(subject);
//
//                // immediately forward
//                subject.observeOn(scheduler).subscribe(response -> proxyNextToSender(request, response));
////
//                proxyProcessRawResponse(request, rawResponse, subject);

                // FIXME
//                rawResponse.toBuilder().setRoute(request.inboxRoute()).build();
                proxyNextToSender(request, rawResponse.toBuilder().setRoute(request.inboxRoute()).build());
//                cache.add(clientId, request, new Message[]{rawResponse});


//                Observer<Message> r = new Observer<Message>() {
//                    @Override
//                    public void onNext(Message response) {
//
////                        Message response = _response.toBuilder().setRoute(request.inboxRoute()).build();
//
//                        System.out.printf("Process complete %s -> %s\n", request, response);
//                        // FIXME super broken but gets it working
//                        // FIXME is create workder broken? exhaust workers or something?
////                        scheduler.createWorker().schedule(() -> {
//                            try {
//                                proxyNextToSender(request, response);
//                                cache.add(clientId, request, new Message[]{response});
//                                proxyCompleteToSender(request);
//                            } catch (Throwable t) {
//                                // FIXME log
//                                t.printStackTrace();
//                            }
////                        });
//                    }
//
//                    @Override
//                    public void onCompleted() {
//                    }
//
//                    @Override
//                    public void onError(Throwable e) {
//                        // FIXME log
//                        e.printStackTrace();
//                        proxyErrorToSender(request);
//                    }
//                };
//                try {
//                    proxyProcessRawResponse(request, rawResponse, r);
//                } catch (Throwable t) {
//                    // FIXME log
//                    t.printStackTrace();
//                }
            }

            @Override
            public void onCompleted() {
//                replayResponses.observeOn(scheduler).subscribe(new Observer<Message>() {
//                    // add to cache
//                    final List<Message> responses = new ArrayList<Message>(4);
//
//                    @Override
//                    public void onNext(Message message) {
//                        responses.add(message);
//                    }
//
//                    @Override
//                    public void onCompleted() {
//                        proxyCompleteToSender(request);
//
//                        cache.add(clientId, request,
//                                responses.toArray(new Message[responses.size()]));
//                    }
//
//                    @Override
//                    public void onError(Throwable e) {
//                        // FIXME log
//                        e.printStackTrace();
//                        proxyErrorToSender(request);
//                    }
//                });

                // FIXME
                proxyCompleteToSender(request);
            }

            @Override
            public void onError(Throwable e) {
                // FIXME log
                e.printStackTrace();
                proxyErrorToSender(request);
            }
        });
    }

    /** @param responseCallback must be thread-safe. It will be called on one executor thread. */
    void proxyProcessRawResponse(Message request, Message rawResponse, Observer<Message> responseCallback) {
        // process the raw response differently depending on type type

        NL.nl.message("proxy.request.forward.process", "Process raw response for %s -> %s", request, rawResponse);

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
        NL.nl.message("proxy.request.next", "Next %s -> %s", request, response);

//        if (!request.inboxRoute().equals(response.route)) {
//            response = response.toBuilder().setRoute(request.inboxRoute()).build();
//        }
        in.send(response);
    }
    void proxyCompleteToSender(Message request) {
        NL.nl.message("proxy.request.complete", "Complete %s", request);
        in.complete(Message.newBuilder().setRoute(request.inboxRoute()).build());

    }
    void proxyErrorToSender(Message request) {
        NL.nl.message("proxy.request.error", "Error %s", request);
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
                if (layers[i].isTransform()) {
                    workExecutor.execute(new ImageTransformWorker(i, memImage));
                } else {
                    emit(i, rawResponse);
                }
            }

            // run the first inline
            if (layers[0].isTransform()) {
                new ImageTransformWorker(0, memImage).run();
            } else {
                emit(0, rawResponse);
            }
        }


        final class ImageTransformWorker implements Runnable {
            int layerIndex;
            BufferedImage memImage;

            ImageTransformWorker(int layerIndex, BufferedImage memImage) {
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


                // FIXME(perf test) borrow the initial byte buffer from a shared pool
                NoCopyByteArrayOutputStream baos = new NoCopyByteArrayOutputStream(8 * 1024);
                try {
                    encode(scaledMemImage, baos, layer);
                } catch (IOException e) {
                    error(e);
                    return;
                }
                // TODO
                // scaledMemImage.dispose();

                // FIXME(perf test) if using a shared pool, copy here
                byte[] buffer = baos.getBytes();
                EncodedImage scaledImage = EncodedImage.create(layer.format, EncodedImage.DEFAULT_ORIENTATION, sw, sh,
                        buffer, baos.getOffset(), baos.getLength());
                // FIXME(perf test) return the byte buffer to a shared pool


                Message.Builder builder = rawResponse.toBuilder()
//                        .setRoute(request.inboxRoute())
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

    static void encode(BufferedImage bi, OutputStream os, Message.LayerInfo layer) throws IOException {
        MemoryCacheImageOutputStream imageOs = new MemoryCacheImageOutputStream(os);
        try {
            _encode(bi, imageOs, layer);
        } finally {
            imageOs.close();
        }
    }

    private static void _encode(BufferedImage bi, ImageOutputStream os, Message.LayerInfo layer) throws IOException {
        ImageWriter imageWriter = ImageIO.getImageWritersByFormatName(layer.format.toString().toLowerCase()).next();
        try {
            ImageWriteParam writeParam = imageWriter.getDefaultWriteParam();
            writeParam.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
            // FIXME values
            switch (layer.quality) {
                case HIGH:
                    writeParam.setCompressionQuality(0.9f);
                    break;
                case LOW:
                    writeParam.setCompressionQuality(0.3f);
                    break;
            }

            imageWriter.setOutput(os);
            imageWriter.write(null, new IIOImage(bi, null, null), writeParam);
        } finally {
            imageWriter.dispose();
        }
    }
}
