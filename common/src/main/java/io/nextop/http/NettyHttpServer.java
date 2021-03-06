package io.nextop.http;

import com.google.common.net.MediaType;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.nextop.ApiComponent;
import io.nextop.ApiException;
import io.nextop.ApiStatus;
import io.nextop.util.Urls;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.Subscription;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.logging.Logger;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.is100ContinueExpected;
import static io.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/** Supports {@link ApiStatus} and {@link ApiException} */
public final class NettyHttpServer extends ApiComponent.Base {
    private static final Logger log = Logger.getGlobal();


    public static final String P_REQUEST = "request";
    public static final String P_REMOTE_ADDRESS = "remote-address";


    public static final class Config {
        public int httpPort = -1;
        // FIXME httpsPort
    }


    private final Scheduler scheduler;
    private final Router router;
    private final Observable<Config> configSource;


    // INTERNAL SUBSCRIPTIONS

    @Nullable
    private Subscription managerSubscription = null;


    public NettyHttpServer(Scheduler scheduler, Router router, Observable<Config> configSource) {
        this.scheduler = scheduler;
        this.router = router;
        this.configSource = configSource;

        NettyManager manager = new NettyManager();
        init = ApiComponent.init("Netty Server",
                statusSink -> {
                    managerSubscription = configSource.take(1).subscribe(manager);
                },
                () -> {
                    managerSubscription.unsubscribe();
                    manager.close();
                });
    }

    private class NettyManager implements Observer<Config> {
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
            log.info(String.format("http listening on port %d", config.httpPort));
        }
        @Override
        public void onCompleted() {
            // ignore
        }
        @Override
        public void onError(Throwable e) {
            // ignore
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
            p.addLast("aggregator", new HttpObjectAggregator(65536));
            p.addLast("handler", new HttpServerHandler());
        }
    }

    private final class HttpServerHandler extends SimpleChannelInboundHandler<Object> {


        @Override
        public void channelReadComplete(ChannelHandlerContext context) {
            context.flush();
        }


        @Override
        protected void messageReceived(ChannelHandlerContext context, Object m) throws Exception {
            if (m instanceof FullHttpRequest) {
                FullHttpRequest request = (FullHttpRequest) m;


                if (is100ContinueExpected(request)) {
                    context.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
                }

                InetAddress remoteAddress = ((InetSocketAddress) context.channel().remoteAddress()).getAddress();

                QueryStringDecoder d = new QueryStringDecoder(request.getUri());

                Map<String, List<?>> parameters = new HashMap<>(d.parameters());
                parameters.put(P_REQUEST, Collections.singletonList(request));
                parameters.put(P_REMOTE_ADDRESS, Collections.singletonList(remoteAddress));

                HttpMethod method = request.getMethod();
                List<String> segments = Urls.parseSegments(d.path());
                drain(router.route(method, segments, parameters), context, isKeepAlive(request));
            }
        }


        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable t) throws Exception {
            // FIXME log
            context.close();
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
                    if (!response.headers().contains(CONTENT_TYPE)) {
                        response.headers().set(CONTENT_TYPE, MediaType.PLAIN_TEXT_UTF_8.toString());
                    }
                    if (!response.headers().contains(CONTENT_LENGTH) && (response instanceof FullHttpResponse)) {
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
}
