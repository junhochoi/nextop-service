package io.nextop.server;


import io.nextop.Id;
import io.nextop.Message;
import io.nextop.WireValue;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import rx.Observable;
import rx.functions.Func1;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

// the cache is responsible for cache-control and determining whether a request can be cached
/** thread-safe */
// FIXME(SECURITY) respect Cache-Control header on response and request
public final class Cache {

    static final int DEFAULT_ENCODE_MAX_BYTES = 8 * 1024 * 1024;
    static final int DEFAULT_MAX_TOTAL_BUFFERS = 4;
    static final int DEFAULT_MAX_IDLE_BUFFERS = 4;
    static final int DEFAULT_MIN_IDLE_BUFFERS = 4;


    final Storage storage;
    final GenericObjectPool<ByteBuffer> encodingBufferPool;


    public Cache(Storage storage) {
        this.storage = storage;


        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxTotal(DEFAULT_MAX_TOTAL_BUFFERS);
        config.setMaxIdle(DEFAULT_MAX_IDLE_BUFFERS);
        config.setMinIdle(DEFAULT_MIN_IDLE_BUFFERS);
        encodingBufferPool = new GenericObjectPool<>(new EncodingBufferFactory(DEFAULT_ENCODE_MAX_BYTES), config);

    }


    // messages to callback must have route=reqeust.inboxRoute
    Observable<Message> probe(Id clientId, Message request) {
        if (!isCacheableRequest(request)) {
            return Observable.error(null);
        }

        String[] orderedKeys = createProbeKeys(clientId, request);

        if (orderedKeys.length <= 0) {
            return Observable.error(null);
        }

        System.out.printf("Cache probe %s\n", Arrays.toString(orderedKeys));
        Observable<byte[]> cacheHit = storage.get(orderedKeys[0]);
        for (int i = 1, n = orderedKeys.length; i < n; ++i) {
            cacheHit = cacheHit.onErrorResumeNext(storage.get(orderedKeys[i]));
        }

        return cacheHit.flatMap(new Func1<byte[], Observable<Message>>() {
            @Override
            public Observable<Message> call(byte[] bytes) {
                System.out.printf("Cache hit %s\n", Arrays.toString(orderedKeys));

                Message[] responses = decode(bytes);
                // reset the routes to match the request
                for (int i = 0, n = responses.length; i < n; ++i) {
                    responses[i] = responses[i].toBuilder()
                            .setRoute(request.inboxRoute())
                            .build();
                }
                return Observable.from(responses);
            }
        });
    }

    public void add(Id clientId, Message request, Message[] responses) {
        System.out.printf("Cache add %s\n", request);

        if (!isCacheableRequest(request)) {
            System.out.printf("Cache add NOT CACHEABLE %s\n", request);
            return;
        }
        for (Message response : responses) {
            if (!isCacheableResponse(response)) {
                System.out.printf("Cache add NOT CACHEABLE %s -> %s\n", request, response);
                return;
            }
        }

        byte[] bytes;
        try {
            bytes = encode(responses);
        } catch (EncodeFailedException e) {
            // FIXME log
            e.printStackTrace();
            // can't store this in the cache; abort
            return;
        }

        String key = createKey(clientId, request, responses);
        System.out.printf("Cache put %s\n", key);
        storage.put(key, bytes);
    }


    private boolean isCacheableRequest(Message request) {
        return Message.isNullipotent(request) && CacheControl.get(request).isCacheable();
    }

    private boolean isCacheableResponse(Message response) {
        return CacheControl.get(response).isCacheable();
    }


    private String[] createProbeKeys(Id clientId, Message request) {
        // FIXME(SECURITY) respect Cache-Control header on response and request

        // FIXME this is so broken. needs at least URI and layer info
        // for now, just use a route
        try {
            return new String[]{request.toUriString()};
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        }
    }
    private String createKey(Id clientId, Message request, Message[] responses) {
        // FIXME(SECURITY) respect Cache-Control header on response and request

        // FIXME this is so broken. needs at least URI and layer info
        // for now, just use a route
        try {
            return request.toUriString();
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        }
    }


    private byte[] encode(Message ... messages) throws EncodeFailedException {
        // FIXME this should strip the route

        int n = messages.length;
        WireValue[] messageValues = new WireValue[n];
        for (int i = 0; i < n; ++i) {
            messageValues[i] = WireValue.of(messages[i]);
        }
        WireValue encodeValue = WireValue.of(Arrays.asList(messageValues));

        ByteBuffer encodingBuffer;
        try {
            encodingBuffer = encodingBufferPool.borrowObject();
        } catch (Exception e) {
            throw new EncodeFailedException(e);
        }

        try {
            encodeValue.toBytes(encodingBuffer);
        } catch (Exception e) {
            // FIXME
            throw new TooLargeException(e);
        }

        encodingBuffer.flip();
        byte[] bytes = new byte[encodingBuffer.remaining()];
        encodingBuffer.get(bytes);
        return bytes;
    }
    private Message[] decode(byte[] bytes) {
        WireValue encodeValue = WireValue.valueOf(bytes);
        List<WireValue> messageValues = encodeValue.asList();
        int n = messageValues.size();
        Message[] messages = new Message[n];
        for (int i = 0; i < n; ++i) {
            messages[i] = messageValues.get(i).asMessage();
        }
        return messages;
    }


    /** async. thread-safe */
    public static interface Storage {
        void put(String key, byte[] bytes);
        Observable<byte[]> get(String key);
    }


    static class EncodeFailedException extends Exception {
        EncodeFailedException(Exception cause) {
            super(cause);
        }
    }
    static class TooLargeException extends EncodeFailedException {
        TooLargeException(Exception cause) {
            super(cause);
        }
    }

    static final class EncodingBufferFactory extends BasePooledObjectFactory<ByteBuffer> {
        final int encodingBufferBytes;

        EncodingBufferFactory(int encodingBufferBytes) {
            this.encodingBufferBytes = encodingBufferBytes;
        }

        @Override
        public ByteBuffer create() throws Exception {
            return ByteBuffer.allocate(encodingBufferBytes);
        }

        @Override
        public void passivateObject(PooledObject<ByteBuffer> p) throws Exception {
            ByteBuffer buffer = p.getObject();
            buffer.reset();
        }

        @Override
        public PooledObject<ByteBuffer> wrap(ByteBuffer buffer) {
            return new DefaultPooledObject<>(buffer);
        }
    }
}
