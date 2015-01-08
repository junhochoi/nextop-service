package io.nextop.service.dns;


import com.google.common.base.Charsets;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.nextop.ApiComponent;
import io.nextop.ApiContainer;
import io.nextop.ApiException;
import io.nextop.ApiStatus;
import io.nextop.rx.MoreRxOperations;
import io.nextop.db.DataSourceProvider;
import io.nextop.http.BasicRouter;
import io.nextop.http.NettyHttpServer;
import io.nextop.http.Router;
import io.nextop.util.ConfigWatcher;
import io.nextop.service.NxId;
import io.nextop.service.Permission;
import io.nextop.service.admin.AdminContext;
import io.nextop.service.admin.AdminModel;
import io.nextop.service.log.ServiceLog;
import io.nextop.service.schema.SchemaController;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.nextop.util.ClUtils.getStrings;


public class DnsService extends ApiComponent.Base {
    private static final Logger localLog = Logger.getGlobal();


    private final Router router() {
        BasicRouter router = new BasicRouter();
        Object accessKeyMatcher = BasicRouter.var("access-key", segment -> NxId.valueOf(segment));

        Function<Map<String, List<?>>, Map<String, List<?>>> validate = parameters -> {
                parameters.put("grant-key", ((List<String>) parameters.getOrDefault("grant-key", Collections.emptyList()
                )).stream().map((String grantKeyString) -> NxId.valueOf(grantKeyString)).collect(Collectors.toList()));
                return parameters;
            };

        router.add(HttpMethod.GET, Arrays.asList(accessKeyMatcher, "overlord"), validate.andThen(parameters -> {
            NxId accessKey = (NxId) parameters.get("access-key").get(0);
            List<NxId> grantKeys = (List<NxId>) parameters.get("grant-key");
            return getOverlords(accessKey, grantKeys);
        }));
        router.add(HttpMethod.GET, Arrays.asList(accessKeyMatcher, "edge"), validate.andThen(parameters -> {
            NxId accessKey = (NxId) parameters.get("access-key").get(0);
            List<NxId> grantKeys = (List<NxId>) parameters.get("grant-key");
            return getEdges(accessKey, grantKeys);
        }));
        router.add(HttpMethod.POST, Arrays.asList(accessKeyMatcher, "overlord"), validate.andThen(parameters -> {
            NxId accessKey = (NxId) parameters.get("access-key").get(0);
            List<NxId> grantKeys = (List<NxId>) parameters.get("grant-key");
            return postOverlords(accessKey, grantKeys);
        }));
        router.add(HttpMethod.POST, Arrays.asList(accessKeyMatcher, "edge"), validate.andThen(parameters -> {
            NxId accessKey = (NxId) parameters.get("access-key").get(0);
            List<NxId> grantKeys = (List<NxId>) parameters.get("grant-key");
            return postEdges(accessKey, grantKeys);
        }));
        router.add(HttpMethod.POST, Arrays.asList(accessKeyMatcher), validate.andThen(parameters -> {
            NxId accessKey = (NxId) parameters.get("access-key").get(0);
            List<NxId> grantKeys = (List<NxId>) parameters.get("grant-key");
            return postAccessKey(accessKey, grantKeys);
        }));

        return router;
    }


    private final Scheduler apiScheduler;
    private final Scheduler modelScheduler;

    private final DataSourceProvider dataSourceProvider;
    private final SchemaController schemaController;
    private final ConfigWatcher configWatcher;
    private final NettyHttpServer httpServer;

    private final AdminContext context;


    DnsService(JsonObject defaultConfigObject, String ... configFiles) {
        apiScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
                        new Thread(r, "DnsService Worker")
        ));
        modelScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
                        new Thread(r, "AdminModel Worker")
        ));

        configWatcher = new ConfigWatcher(modelScheduler, defaultConfigObject, configFiles);
        dataSourceProvider = new DataSourceProvider(modelScheduler,
                configWatcher.getMergedObservable().map(configObject -> {
                    JsonObject dbConfigObject = configObject.get("db").getAsJsonObject();

                    DataSourceProvider.Config config = new DataSourceProvider.Config();
                    config.scheme = dbConfigObject.get("scheme").getAsString();
                    config.host = dbConfigObject.get("host").getAsString();
                    config.port = dbConfigObject.get("port").getAsInt();
                    config.db = dbConfigObject.get("db").getAsString();
                    config.user = dbConfigObject.get("user").getAsString();
                    config.password = dbConfigObject.get("password").getAsString();
                    return config;
                }));
        schemaController = new SchemaController(dataSourceProvider);
        httpServer = new NettyHttpServer(apiScheduler, router(),
                configWatcher.getMergedObservable().map(configObject -> {
                    JsonObject httpConfigObject = configObject.get("http").getAsJsonObject();

                    // FIXME fix the parsing here
                    NettyHttpServer.Config config = new NettyHttpServer.Config();
                    config.httpPort = httpConfigObject.get("port").getAsInt();
                    return config;
                }));

        context = new AdminContext();
        context.scheduler = modelScheduler;
        context.log = new ServiceLog();
        context.dataSourceProvider = dataSourceProvider;
        context.adminModel = new AdminModel(context);

        init = ApiComponent.layerInit(configWatcher.init(),
                dataSourceProvider.init(),
                schemaController.init(),
                ApiComponent.init("Admin Schema Upgrade",
                        statusSink -> {
                            MoreRxOperations.blockingSubscribe(schemaController.justUpgrade("admin"), statusSink);
                        },
                        () -> {}),
                context.init(),
                httpServer.init());
    }


    /////// ROUTES ///////

    private Observable<HttpResponse> getOverlords(NxId accessKey, Collection<NxId> grantKeys) {
        return context.adminModel.requirePermissions(context.adminModel.justOverlords(accessKey), accessKey, grantKeys,
                Permission.admin.on()).map(authorities -> {
            String joinedAuthorityStrings = authorities.stream().map(authority -> authority.toString()).collect(Collectors.joining(";"));

            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                    Unpooled.copiedBuffer(joinedAuthorityStrings.getBytes(Charsets.UTF_8)));
        });
    }

    private Observable<HttpResponse> postOverlords(NxId accessKey, Collection<NxId> grantKeys) {
        return context.adminModel.requirePermissions(context.adminModel.justDirtyOverlords(accessKey), accessKey, grantKeys,
                Permission.admin.on()).map(apiResponse -> {

            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);
        });
    }

    private Observable<HttpResponse> getEdges(NxId accessKey, Collection<NxId> grantKeys) {
        return context.adminModel.justOverlords(accessKey).map(authorities -> {
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

        try {
            main(new GnuParser().parse(options, in));
        } catch (Exception e) {
            localLog.log(Level.SEVERE, "dns.main", e);
            new HelpFormatter().printHelp("dns", options);
            System.exit(400);
        }
    }
    private static void main(CommandLine cl) throws Exception {
        JsonObject defaultConfigObject = getDefaultConfigObject();
        String[] configFiles = getStrings(cl, 'c', new String[0]);

        DnsService dnsService = new DnsService(defaultConfigObject, configFiles);

        Stream.of(cl.getArgs()).map(String::toLowerCase).forEach(arg -> {
            switch (arg) {
                case "start":
                    new ApiContainer(dnsService).start(status -> {
                        localLog.log(Level.INFO, String.format("%-20s %s", "dns.main.init", status));
                    });
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        });
    }
    private static JsonObject getDefaultConfigObject() throws IOException {
        // extract the default (bundled) config object
        Reader r = new BufferedReader(new InputStreamReader(ClassLoader.getSystemClassLoader().getResourceAsStream("conf.json"), Charsets.UTF_8));
        try {
            return new JsonParser().parse(r).getAsJsonObject();
        } finally {
            r.close();
        }
    }
}
