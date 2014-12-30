package io.nextop.service.dns;


import com.google.common.base.Charsets;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.nextop.rx.util.ConfigWatcher;
import io.nextop.service.*;
import org.apache.commons.cli.*;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

import javax.annotation.Nullable;
import java.io.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;

import static io.nextop.service.log.ServiceLog.log;


public class DnsService {
    private final Scheduler dnsScheduler;
    private final Scheduler modelScheduler;

    private final ConfigWatcher configWatcher;
    private final ServiceAdminModel serviceAdminModel;

    @Nullable
    private Subscription serverSubscription = null;


    DnsService(JsonObject defaultConfigObject, String ... configFiles) {
        dnsScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
            new Thread(r, "DnsService Worker")
        ));
        modelScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
                        new Thread(r, "ServiceAdminModel Worker")
        ));

        configWatcher = new ConfigWatcher(dnsScheduler, defaultConfigObject, configFiles);
        serviceAdminModel = new ServiceAdminModel(modelScheduler, configWatcher.getMergedObservable().map(configObject ->
                configObject.get("adminModel").getAsJsonObject()));
    }


    public void start()  {
        log.count("dns.start");

        if (null == serverSubscription) {
            serverSubscription = configWatcher.getMergedObservable().subscribe(new Observer<JsonObject>() {
                @Nullable EventLoopGroup bossGroup = null;
                @Nullable EventLoopGroup workerGroup = null;
                @Nullable Channel channel = null;


                @Override
                public void onNext(JsonObject configObject) {
                    log.message("dns.start.config", "%s", configObject);

                    close();
                    assert null == bossGroup;
                    assert null == workerGroup;
                    assert null == channel;

                    int port = configObject.get("httpPort").getAsInt();

                    bossGroup = new NioEventLoopGroup();
                    workerGroup = new NioEventLoopGroup();
                    ServerBootstrap b = new ServerBootstrap();
                    b.option(ChannelOption.SO_BACKLOG, 1024);
                    b.group(bossGroup, workerGroup)
                            .channel(NioServerSocketChannel.class)
                            .childHandler(new HttpServerInitializer(configObject));

                    channel = b.bind(port).syncUninterruptibly().channel();
                    log.message("dns.start", "listening on port %d", port);
                }
                @Override
                public void onCompleted() {
                    close();
                }
                @Override
                public void onError(Throwable e) {
                    log.unhandled("dns.start", e);
                    close();
                }


                void close() {
                    log.message("dns.start.close");

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

    private Observable<HttpResponse> route(HttpMethod method, List<String> segments, Map<String, List<String>> parameters) {
        log.message("dns.route", "%s %s %s", method, segments, parameters);


        // parse and validate path+params
        NxId accessKey;
        List<NxId> grantKeys;

        if (segments.isEmpty()) {
            return errorRoute(method, segments, parameters);
        }
        try {
            accessKey = NxId.valueOf(segments.get(0));
        } catch (IllegalArgumentException e) {
            return errorRoute(method, segments, parameters);
        }
        try {
            grantKeys = parameters.getOrDefault("grant-key", Collections.<String>emptyList()).stream().map((String grantKeyString) -> NxId.valueOf(grantKeyString)).collect(Collectors.toList());
        } catch (IllegalArgumentException e) {
            return errorRoute(method, segments, parameters);
        }

        log.message("");

        if (2 == segments.size() && HttpMethod.GET.equals(method) && "overlord".equals(segments.get(1))) {
            return getOverlords(accessKey, grantKeys);
        } else if (2 == segments.size() && HttpMethod.GET.equals(method) && "edge".equals(segments.get(1))) {
            return getEdges(accessKey, grantKeys);
        } else if (2 == segments.size() && HttpMethod.POST.equals(method) && "overlord".equals(segments.get(1))) {
            return postOverlords(accessKey, grantKeys);
        } else if (2 == segments.size() && HttpMethod.POST.equals(method) && "edge".equals(segments.get(1))) {
            return postEdges(accessKey, grantKeys);
        } else if (1 == segments.size() && HttpMethod.POST.equals(method)) {
            return postAccessKey(accessKey, grantKeys);
        } else {
            return errorRoute(method, segments, parameters);
        }
    }
    private Observable<HttpResponse> errorRoute(HttpMethod method, List<String> segments, Map<String, List<String>> parameters) {
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST));
    }


    private Observable<HttpResponse> getOverlords(NxId accessKey, Collection<NxId> grantKeys) {
        return serviceAdminModel.requirePermissions(serviceAdminModel.justOverlords(accessKey), accessKey, grantKeys,
                Permission.admin.on()).map(authorities -> {
                    String joinedAuthorityStrings = authorities.stream().map(authority -> authority.toString()).collect(Collectors.joining(";"));

                    return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                            Unpooled.copiedBuffer(joinedAuthorityStrings.getBytes(Charsets.UTF_8)));
                });
    }

    private Observable<HttpResponse> postOverlords(NxId accessKey, Collection<NxId> grantKeys) {
        return serviceAdminModel.requirePermissions(serviceAdminModel.justDirtyOverlords(accessKey), accessKey, grantKeys,
                Permission.admin.on()).map(apiResponse -> {

            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);
        });
    }

    private Observable<HttpResponse> getEdges(NxId accessKey, Collection<NxId> grantKeys) {
        return serviceAdminModel.justOverlords(accessKey).map(authorities -> {
            String joinedAuthorityStrings = authorities.stream().map(authority -> authority.toString()).collect(Collectors.joining(";"));

            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                    Unpooled.copiedBuffer(joinedAuthorityStrings.getBytes(Charsets.UTF_8)));
        });
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
                drain(route(r.getMethod(), parseSegments(d.path()), d.parameters()), context, isKeepAlive(r));
            }
        }


        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            // FIXME log
            ctx.close();
        }


        void drain(Observable<HttpResponse> responseSource, ChannelHandlerContext context, boolean keepAlive) {
            responseSource.subscribe(new Observer<HttpResponse>() {



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
        List<String> segments = new ArrayList<>(n);
        for (int i = j; i <= length; ++i) {
            if (length == i || '/' == path.charAt(i)) {
                segments.add(path.substring(j, i));
                j = i + 1;
            }
        }

        return segments;
    }


    /////// MAIN ///////

    public static void main(String[] in) {
        Options options = new Options();
        options.addOption("c", "configFile", true, "JSON config file");

        CommandLine cl;
        try {
            main(new GnuParser().parse(options, in));
        } catch (Exception e) {
            log.unhandled("dns.main", e);
            new HelpFormatter().printHelp("dns", options);
            System.exit(400);
        }
    }
    private static void main(CommandLine cl) throws Exception {
        JsonObject defaultConfigObject;
        // extract the default (bundled) config object
        Reader r = new BufferedReader(new InputStreamReader(ClassLoader.getSystemClassLoader().getResourceAsStream("conf.json"), Charsets.UTF_8));
        try {
            defaultConfigObject = new JsonParser().parse(r).getAsJsonObject();
        } finally {
            r.close();
        }

        @Nullable String[] configFiles = cl.getOptionValues('c');

        Stream.of(cl.getArgs()).map(String::toLowerCase).forEach(arg -> {
            if ("start".equals(arg)) {
                DnsService dnsService = new DnsService(defaultConfigObject, null != configFiles ? configFiles : new String[0]);
                dnsService.start();
            } else {
                throw new IllegalArgumentException();
            }
        });
    }
}
