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
import io.nextop.rx.http.BasicRouter;
import io.nextop.rx.http.NettyServer;
import io.nextop.rx.http.Router;
import io.nextop.rx.util.ConfigWatcher;
import io.nextop.service.*;
import org.apache.commons.cli.*;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Func1;
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
    private final Router router() {
        BasicRouter router = new BasicRouter();
        Object accessKeyMatcher = BasicRouter.var("access-key", segment -> NxId.valueOf(segment));

        // FIXME is there a more java interface for this?
        Func1<Func1<Map<String, List<?>>, Observable<HttpResponse>>, Func1<Map<String, List<?>>, Observable<HttpResponse>>> validate = (Func1<Map<String, List<?>>, Observable<HttpResponse>> responseGenerator) ->
            (Map<String, List<?>> parameters) -> {
                parameters.put("grant-key", ((List<String>) parameters.getOrDefault("grant-key", Collections.<String>emptyList()
                )).stream().map((String grantKeyString) -> NxId.valueOf(grantKeyString)).collect(Collectors.toList()));

                return responseGenerator.call(parameters);
            };

        router.add(HttpMethod.GET, Arrays.asList(accessKeyMatcher, "overlord"), validate.call(parameters -> {
            NxId accessKey = (NxId) parameters.get("access-key").get(0);
            List<NxId> grantKeys = (List<NxId>) parameters.get("grant-key");
            return getOverlords(accessKey, grantKeys);
        }));
        router.add(HttpMethod.GET, Arrays.asList(accessKeyMatcher, "edge"), validate.call(parameters -> {
            NxId accessKey = (NxId) parameters.get("access-key").get(0);
            List<NxId> grantKeys = (List<NxId>) parameters.get("grant-key");
            return getEdges(accessKey, grantKeys);
        }));
        router.add(HttpMethod.POST, Arrays.asList(accessKeyMatcher, "overlord"), validate.call(parameters -> {
            NxId accessKey = (NxId) parameters.get("access-key").get(0);
            List<NxId> grantKeys = (List<NxId>) parameters.get("grant-key");
            return postOverlords(accessKey, grantKeys);
        }));
        router.add(HttpMethod.POST, Arrays.asList(accessKeyMatcher, "edge"), validate.call(parameters -> {
            NxId accessKey = (NxId) parameters.get("access-key").get(0);
            List<NxId> grantKeys = (List<NxId>) parameters.get("grant-key");
            return postEdges(accessKey, grantKeys);
        }));
        router.add(HttpMethod.POST, Arrays.asList(accessKeyMatcher), validate.call(parameters -> {
            NxId accessKey = (NxId) parameters.get("access-key").get(0);
            List<NxId> grantKeys = (List<NxId>) parameters.get("grant-key");
            return postAccessKey(accessKey, grantKeys);
        }));

        return router;
    }


    private final Scheduler dnsScheduler;
    private final Scheduler modelScheduler;

    private final ConfigWatcher configWatcher;
    private final ServiceAdminModel serviceAdminModel;

    private final NettyServer httpServer;


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

        httpServer = new NettyServer(dnsScheduler, router());
    }


    public void start()  {

        httpServer.start(configWatcher.getMergedObservable().map(configObject -> {
            NettyServer.Config config = new NettyServer.Config();
            config.httpPort = configObject.get("httpPort").getAsInt();
            return config;
        }));

    }

    public void stop() {
        httpServer.stop();
    }


    /////// ROUTES ///////

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
