package io.nextop.rx.http;

import com.google.gson.JsonObject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.nextop.service.ApiException;
import io.nextop.service.ApiStatus;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.Subscription;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.is100ContinueExpected;
import static io.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.nextop.service.log.ServiceLog.log;

/** Supports {@link ApiStatus} and {@link ApiException} */
public final class NettyServer {
    public static final class Config {
        public int httpPort = -1;
        // FIXME httpsPort
    }


    private final Scheduler scheduler;
    private final Router router;


    // INTERNAL SUBSCRIPTIONS

    @Nullable
    private Subscription configSubscription = null;


    public NettyServer(Scheduler scheduler, Router router) {
        this.scheduler = scheduler;
        this.router = router;
    }



    public void start(Observable<Config> configSource) {
        configSource.subscribeOn(scheduler
        ).subscribe(new Observer<Config>() {
            @Nullable EventLoopGroup bossGroup = null;
            @Nullable EventLoopGroup workerGroup = null;
            @Nullable Channel channel = null;


            @Override
            public void onNext(Config config) {
                close();
                assert null == bossGroup;
                assert null == workerGroup;
                assert null == channel;

                // HTTP
                bossGroup = new NioEventLoopGroup();
                workerGroup = new NioEventLoopGroup();
                ServerBootstrap b = new ServerBootstrap();
                b.option(ChannelOption.SO_BACKLOG, 1024);
                b.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(new HttpServerInitializer());

                channel = b.bind(config.httpPort).syncUninterruptibly().channel();
            }
            @Override
            public void onCompleted() {
                close();
            }
            @Override
            public void onError(Throwable e) {
                close();
            }


            void close() {
                if (null != channel) {
                    channel.close();
                    channel = null;
                }
                if (null != workerGroup) {
                    workerGroup.shutdownGracefully();
                    workerGroup = null;
                }
                if (null != bossGroup) {
                    bossGroup.shutdownGracefully();
                    bossGroup = null;
                }


            }
        });
    }

    public void stop() {
        // FIXME
    }



    // FIXME move all of this including routes into some simple HTTP(S) server object

    private final class HttpServerInitializer extends ChannelInitializer<SocketChannel> {
        HttpServerInitializer() {
            // FIXME
        }

        @Override
        public void initChannel(SocketChannel channel) throws Exception {
            ChannelPipeline p = channel.pipeline();
            // FIXME
            // Uncomment the following line if you want HTTPS
            //SSLEngine engine = SecureChatSslContextFactory.getServerContext().createSSLEngine();
            //engine.setUseClientMode(false);
            //permission.addLast("ssl", new SslHandler(engine));

            p.addLast("codec", new HttpServerCodec());
            p.addLast("handler", new HttpServerHandler());
        }
    }

    private final class HttpServerHandler extends ChannelHandlerAdapter {
        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        // FIXME
        @Override
        public void channelRead(ChannelHandlerContext context, Object m) throws Exception {
            if (m instanceof HttpRequest) {
                HttpRequest r = (HttpRequest) m;

                if (is100ContinueExpected(r)) {
                    context.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
                }

                QueryStringDecoder d = new QueryStringDecoder(r.getUri());
                HttpMethod method = r.getMethod();
                List<String> segments = parseSegments(d.path());
                Map<String, List<?>> parameters = new HashMap<>(d.parameters());
                drain(router.route(method, segments, parameters), context, isKeepAlive(r));
            }
        }


        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            // FIXME log
            ctx.close();
        }


        void drain(Observable<HttpResponse> responseSource, ChannelHandlerContext context, boolean keepAlive) {
            responseSource.subscribeOn(scheduler
            ).subscribe(new Observer<HttpResponse>() {



                @Nullable
                HttpResponse headResponse = null;


                @Override
                public void onNext(HttpResponse response) {
                    headResponse = response;
                }

                @Override
                public void onError(Throwable t) {
                    HttpResponse r;
                    if (t instanceof ApiException) {
                        ApiStatus status = ((ApiException) t).status;
                        r = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(status.code,
                                null != status.reason ? status.reason : ""));
                    } else {
                        r = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                    }
                    send(r);
                }

                @Override
                public void onCompleted() {
                    HttpResponse r;
                    if (null != headResponse) {
                        r = headResponse;
                    } else {
                        r = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);
                    }
                    send(r);
                }


                void send(HttpResponse response) {
                    log.message("dns.send", "%s", response);

                    response.headers().set(CONTENT_TYPE, "text/plain");
                    if (response instanceof FullHttpResponse) {
                        response.headers().set(CONTENT_LENGTH, ((FullHttpResponse) response).content().readableBytes());
                    }
                    if (keepAlive) {
                        response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
                        context.write(response);
                    } else {
                        context.write(response
                        ).addListener(ChannelFutureListener.CLOSE);
                    }
                    context.flush();
                }

            });


        }
    }



    /** @param path leading '/' may be omitted */
    private static List<String> parseSegments(String path) {
        int j;
        if ('/' == path.charAt(0)) {
            j = 1;
        } else {
            j = 0;
        }

        final int length = path.length();
        int n = 0;

        for (int i = j; i <= length; ++i) {
            if (length == i || '/' == path.charAt(i)) {
                ++n;
            }
        }
        String[] segments = new String[n];
        n = 0;
        for (int i = j; i <= length; ++i) {
            if (length == i || '/' == path.charAt(i)) {
                segments[n++] = path.substring(j, i);
                j = i + 1;
            }
        }
        assert n == segments.length;
        return Arrays.asList(segments);
    }
}