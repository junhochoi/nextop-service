package io.nextop.service.dns;


import com.google.common.base.Charsets;
import com.google.gson.JsonObject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.nextop.rx.util.ConfigWatcher;
import io.nextop.service.NxId;
import io.nextop.service.Permission;
import io.nextop.service.ServiceModel;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;


public class DnsService {
    private final Scheduler dnsScheduler;
    private final Scheduler modelScheduler;

    private final ConfigWatcher configWatcher;
    private final ServiceModel serviceModel;

    @Nullable
    private Subscription serverSubscription = null;


    DnsService(String ... configFiles) {
        dnsScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
            new Thread(r, "DNSService Worker")
        ));
        modelScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
                        new Thread(r, "ServiceModel Worker")
        ));

        configWatcher = new ConfigWatcher(dnsScheduler, configFiles);
        serviceModel = new ServiceModel(modelScheduler, configWatcher.getMergedObservable());
    }


    public void start()  {
        if (null == serverSubscription) {
            serverSubscription = configWatcher.getMergedObservable().subscribe(new Observer<JsonObject>() {
                @Nullable EventLoopGroup bossGroup = null;
                @Nullable EventLoopGroup workerGroup = null;
                @Nullable ChannelFuture channelf = null;


                @Override
                public void onNext(JsonObject configObject) {
                    close();
                    assert null == bossGroup;
                    assert null == workerGroup;
                    assert null == channelf;

                    int port = configObject.get("httpPort").getAsInt();

                    bossGroup = new NioEventLoopGroup();
                    workerGroup = new NioEventLoopGroup();
                    try {
                        ServerBootstrap b = new ServerBootstrap();
                        b.option(ChannelOption.SO_BACKLOG, 1024);
                        b.group(bossGroup, workerGroup)
                                .channel(NioServerSocketChannel.class)
                                .childHandler(new HttpServerInitializer(configObject));

                        channelf = b.bind(port);
                    } finally {
                        bossGroup.shutdownGracefully();
                        workerGroup.shutdownGracefully();
                    }
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
                    if (null != bossGroup) {
                        bossGroup.shutdownGracefully();
                        bossGroup = null;
                    }
                    if (null != workerGroup) {
                        workerGroup.shutdownGracefully();
                        workerGroup = null;
                    }
                    if (null != channelf) {
                        channelf.addListener(ChannelFutureListener.CLOSE);
                        channelf = null;
                    }
                }
            });
        }
    }

    public void stop() {
        if (null != serverSubscription) {
            // FIXME this should call close on the server
            // FIXME test
            serverSubscription.unsubscribe();
            serverSubscription = null;
        }
    }


    /////// ROUTES ///////

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
            grantKeys = parameters.getOrDefault("grant-key", Collections.<String>emptyList()).stream().map((String grantKeyString) -> NxId.valueOf(grantKeyString)).collect(Collectors.toList());
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
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST));
    }


    private Observable<HttpResponse> getOverlords(NxId accessKey, Collection<NxId> grantKeys) {
        return serviceModel.requirePermissions(serviceModel.justOverlords(accessKey), accessKey, grantKeys,
                Permission.admin.on()).map(authorities -> {
                    String joinedAuthorityStrings = authorities.stream().map(authority -> authority.toString()).collect(Collectors.joining(";"));

                    return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                            Unpooled.copiedBuffer(joinedAuthorityStrings.getBytes(Charsets.UTF_8)));
                });
    }

    private Observable<HttpResponse> postOverlords(NxId accessKey, Collection<NxId> grantKeys) {
        return serviceModel.requirePermissions(serviceModel.justDirtyOverlords(accessKey), accessKey, grantKeys,
                Permission.admin.on()).map(apiResponse -> {

            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);
        });
    }

    private Observable<HttpResponse> getEdges(NxId accessKey, Collection<NxId> grantKeys) {
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
    }

    private Observable<HttpResponse> postEdges(NxId accessKey, Collection<NxId> grantKeys) {
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
    }

    private Observable<HttpResponse> postAccessKey(NxId accessKey, Collection<NxId> grantKeys) {
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
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
        // FIXME logging
        System.out.printf("DNS\n");


        // TODO opts
        String configFile = "./conf.json";

        new DnsService(configFile).start();
    }
}
