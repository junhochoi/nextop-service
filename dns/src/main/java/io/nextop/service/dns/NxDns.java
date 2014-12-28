package io.nextop.service.dns;


import com.google.gson.JsonObject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.nextop.service.ConfigWatcher;
import io.nextop.service.NxId;
import io.nextop.service.Permission;
import io.nextop.service.ServiceData;
import rx.Observable;
import rx.Observer;
import rx.Subscription;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;


public class NxDns {
    // FIXME exector services for server and data

    private final ConfigWatcher configWatcher;
    private final ServiceData serviceData;

    @Nullable
    private Subscription serverSubscription = null;


    NxDns(String configFile) {
        configWatcher = new ConfigWatcher(configFile);
        sd = new ServiceData(dataScheduler, configWatcher);
    }


    public void start()  {
        if (null == serverSubscription) {
            serverSubscription = configWatcher.subscribe({
                    // FIXME previous server

                    (JsonObject config) -> {
                        EventLoopGroup bossGroup = new NioEventLoopGroup();
                        EventLoopGroup workerGroup = new NioEventLoopGroup();
                        try {
                            ServerBootstrap b = new ServerBootstrap();
                            b.option(ChannelOption.SO_BACKLOG, 1024);
                            b.group(bossGroup, workerGroup)
                                    .channel(NioServerSocketChannel.class)
                                    .childHandler(new HttpServerInitializer(config));

                            Channel ch = b.bind(port).sync().channel();
                            ch.closeFuture().sync();
                        } finally {
                            bossGroup.shutdownGracefully();
                            workerGroup.shutdownGracefully();
                        }
                    }
            });
        }
    }

    public void stop() {
        if (null != serverSubscription) {
            // FIXME this should call close on the server
            serverSubscription.unsubscribe();
            serverSubscription = null;
        }
    }


    /////// DNS ///////

    private Observable<HttpResponse> route(HttpMethod method, String path, Map<String, List<String>> parameters) {
        String[] segments = path.split("/");

        // parse and validate path+params
        NxId accessKey;
        List<NxId> grantKeys;

        if (segments.length <= 0) {
            return errorRoute(method, path, parameters);
        }
        try {
            accessKey = NxId.valueOf(segments[0]);
        } catch (IllegalArgumentException e) {
            return errorRoute(method, path, parameters);
        }
        try {
            grantKeys = parameters.getOrDefault("grant-key", Collections.<String>emptyList()).map(grantKeyString -> NxId.valueOf(grantKeyString));
        } catch (IllegalArgumentException e) {
            return errorRoute(method, path, parameters);
        }


        if (2 == segments.length && HttpMethod.GET.equals(method) && "overlord".equals(segments[1])) {
            return getOverlords(accessKey, grantKeys);
        } else if (2 == segments.length && HttpMethod.GET.equals(method) && "edge".equals(segments[1])) {
            return getEdges(accessKey, grantKeys);
        } else if (2 == segments.length && HttpMethod.POST.equals(method) && "overlord".equals(segments[1])) {
            return postOverlords(accessKey, grantKeys);
        } else if (2 == segments.length && HttpMethod.POST.equals(method) && "edge".equals(segments[1])) {
            return postEdges(accessKey, grantKeys);
        } else if (1 == segments.length && HttpMethod.POST.equals(method)) {
            return postAccessKey(accessKey, grantKeys);
        } else {
            return errorRoute(method, path, parameters);
        }
    }
    private Observable<HttpResponse> errorRoute(HttpMethod method, String path, Map<String, List<String>> parameters) {
        // FIXME
    }


    private Observable<HttpResponse> getOverlords(NxId accessKey, Iterable<NxId> grantKeys) {
        return serviceData.requirePermissions(serviceData.justOverlords(accessKey, true), accessKey, grantKeys,
                Permission.admin.on()).map(authorities -> {
                    String joinedAuthorities = authorities.map(authority -> authority.toString()).join(";");
                    // FIXME HTTP response
                });
    }

    private Observable<HttpResponse> postOverlords(NxId accessKey, Iterable<NxId> grantKeys) {
        return serviceData.requirePermissions(serviceData.justDirtyOverlords(accessKey), accessKey, grantKeys,
                Permission.admin.on()).map(apiResponse -> {
            // FIXME HTTP response
        });
    }

    private Observable<HttpResponse> getEdges(NxId accessKey, Iterable<NxId> grantKeys) {

    }

    private Observable<HttpResponse> postEdges(NxId accessKey, Iterable<NxId> grantKeys) {

    }

    private Observable<HttpResponse> postAccessKey(NxId accessKey, Iterable<NxId> grantKeys) {

    }







    // FIXME move all of this including routes into some simple HTTP(S) server object

    private final class HttpServerInitializer extends ChannelInitializer<SocketChannel> {
        HttpServerInitializer(JsonObject configObj) {
            // FIXME
        }

        @Override
        public void initChannel(SocketChannel channel) throws Exception {
            ChannelPipeline p = channel.pipeline();
            // FIXME
            // Uncomment the following line if you want HTTPS
            //SSLEngine engine = SecureChatSslContextFactory.getServerContext().createSSLEngine();
            //engine.setUseClientMode(false);
            //p.addLast("ssl", new SslHandler(engine));

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
                drain(route(r.getMethod(), d.path(), d.parameters()), context, isKeepAlive(r));
            }
        }


        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            // FIXME log
            ctx.close();
        }


        void drain(Observable<HttpResponse> responseSource, final ChannelHandlerContext context, final boolean keepAlive) {
            responseSource.subscribe(new Observer<HttpResponse>() {



                @Nullable HttpResponse headResponse = null;

                @Override
                public void onNext(HttpResponse response) {
                    if (null != headResponse) {
                        // FIXME error
                    }
                    headResponse = response;
                }

                @Override
                public void onError(Throwable t) {
                    headResponse = /* FIXME error response */ null;
                    send(headResponse);
                }

                @Override
                public void onCompleted() {
                    if (null == headResponse) {
                        // FIXME generate empty error response (temp unavailable)
                    }
                    send(headResponse);
                }


                void send(HttpResponse response) {
                    if (keepAlive) {
                        response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
                        context.write(response);
                    } else {
                        context.write(response).addListener(ChannelFutureListener.CLOSE);
                    }
                }

            });


        }
    }



    /////// MAIN ///////

    public static void main(String[] in) {
        // TODO opts
        String configFile = "./conf.json";

        new NxDns(configFile).start();
    }
}
