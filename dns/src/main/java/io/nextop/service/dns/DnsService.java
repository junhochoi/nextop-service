package io.nextop.service.dns;


import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.squareup.pagerduty.incidents.PagerDuty;
import com.squareup.pagerduty.incidents.Trigger;
import io.netty.handler.codec.http.*;
import io.nextop.*;
import io.nextop.db.DataSourceProvider;
import io.nextop.http.BasicRouter;
import io.nextop.http.NettyHttpServer;
import io.nextop.http.Router;
import io.nextop.rx.MoreRxOperations;
import io.nextop.service.Permission;
import io.nextop.service.admin.AdminContext;
import io.nextop.service.admin.AdminModel;
import io.nextop.service.log.ServiceLog;
import io.nextop.service.m.BadAuthority;
import io.nextop.service.m.Overlord;
import io.nextop.service.schema.SchemaController;
import io.nextop.util.CliUtils;
import io.nextop.util.ConfigWatcher;
import io.nextop.util.NettyUtils;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;


public class DnsService extends ApiComponent.Base {
    private static final Logger localLog = Logger.getGlobal();


    private final Router router() {
        BasicRouter router = new BasicRouter();
        Object accessKeyMatcher = BasicRouter.var("access-key", segment -> Id.valueOf(segment));

        Function<Map<String, List<?>>, Map<String, List<?>>> validate = parameters -> {
                parameters.put("grant-key", ((List<String>) parameters.getOrDefault("grant-key", Collections.emptyList()
                )).stream().map((String grantKeyString) -> Id.valueOf(grantKeyString)).collect(Collectors.toList()));
                return parameters;
            };
        Function<Map<String, List<?>>, Map<String, List<?>>> validateBadAuthorities = parameters -> {
            Ip remote = Ip.valueOf((InetAddress) parameters.get(NettyHttpServer.P_REMOTE_ADDRESS).get(0));

            FullHttpRequest request = (FullHttpRequest) parameters.get(NettyHttpServer.P_REQUEST).get(0);

            JsonObject bodyObject = new JsonParser().parse(request.content().toString(Charsets.UTF_8)).getAsJsonObject();

            List<BadAuthority> badAuthorities = new LinkedList<>();
            JsonArray badAuthoritiesArray = bodyObject.get("badAuthorities").getAsJsonArray();
            for (int i = 0, n = badAuthoritiesArray.size(); i < n; ++i) {
                JsonObject badAuthorityObject = badAuthoritiesArray.get(i).getAsJsonObject();

                Authority authority = Authority.valueOf(badAuthorityObject.get("authority").getAsString());

                IntSet schemes = new IntOpenHashSet(3);
                JsonArray schemesArray = badAuthorityObject.get("schemes").getAsJsonArray();
                for (int j = 0, m = schemesArray.size(); j < m; ++j) {
                    schemes.add(schemesArray.get(j).getAsInt());
                }

                BadAuthority badAuthority = new BadAuthority();
                badAuthority.reporter = remote;
                badAuthority.authority = authority;
                badAuthority.schemes = schemes;
                badAuthorities.add(badAuthority);
            }

            parameters.put("bad-authorities", badAuthorities);

            return parameters;
        };

        router.add(HttpMethod.GET, Arrays.asList(accessKeyMatcher, "overlord.json"), validate.andThen(parameters -> {
            Id accessKey = (Id) parameters.get("access-key").get(0);
            List<Id> grantKeys = (List<Id>) parameters.get("grant-key");
            return getOverlords(accessKey, grantKeys);
        }));
        router.add(HttpMethod.GET, Arrays.asList(accessKeyMatcher, "edge.json"), validate.andThen(parameters -> {
            Id accessKey = (Id) parameters.get("access-key").get(0);
            List<Id> grantKeys = (List<Id>) parameters.get("grant-key");
            return getEdges(accessKey, grantKeys);
        }));
        router.add(HttpMethod.POST, Arrays.asList(accessKeyMatcher, "edge.json"), validate.andThen(validateBadAuthorities
        ).andThen(parameters -> {
            Id accessKey = (Id) parameters.get("access-key").get(0);
            List<Id> grantKeys = (List<Id>) parameters.get("grant-key");
            List<BadAuthority> badAuthorities = (List<BadAuthority>) parameters.get("bad-authorities");
            return postEdges(accessKey, grantKeys, badAuthorities);
        }));
        router.add(HttpMethod.POST, Arrays.asList(accessKeyMatcher, "overlord"), validate.andThen(parameters -> {
            Id accessKey = (Id) parameters.get("access-key").get(0);
            List<Id> grantKeys = (List<Id>) parameters.get("grant-key");
            return postOverlords(accessKey, grantKeys);
        }));
        router.add(HttpMethod.POST, Arrays.asList(accessKeyMatcher), validate.andThen(parameters -> {
            Id accessKey = (Id) parameters.get("access-key").get(0);
            List<Id> grantKeys = (List<Id>) parameters.get("grant-key");
            return postAccessKey(accessKey, grantKeys);
        }));

        return router;
    }


    private final Scheduler apiScheduler;
    private final Scheduler modelScheduler;

    // FIXME combine upgrade and the DB provider so that the connection is exposed to the outside once the upgrade finishes
    private final DataSourceProvider dataSourceProvider;
    private final SchemaController schemaController;
    private final ConfigWatcher configWatcher;
    private final NettyHttpServer httpServer;

    private final AdminContext context;

    @Nullable
    private PagerDuty pagerDuty = null;


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
                        () -> {
                        }),
                ApiComponent.init("Pager Duty",
                        statusSink -> {
                            configWatcher.getMergedObservable().subscribe(configObject -> {
                                try {
                                    pagerDuty = PagerDuty.create(configObject.get("pagerduty").getAsJsonObject().get("apiKey").getAsString());
                                } catch (Exception e) {
                                    // not configured
                                }
                            });
                        },
                        () -> {
                        }),
                context.init(),
                httpServer.init());
    }


    /////// ROUTES ///////

    private Observable<HttpResponse> getOverlords(Id accessKey, Collection<Id> grantKeys) {
        // FIXME
        return context.adminModel.requirePermissions(getEdges(accessKey, grantKeys),
                accessKey, grantKeys, Permission.admin.on());
    }

    private Observable<HttpResponse> postOverlords(Id accessKey, Collection<Id> grantKeys) {
        return context.adminModel.requirePermissions(context.adminModel.justDirtyOverlords(accessKey), accessKey, grantKeys,
                Permission.admin.on()).map(apiResponse -> {

            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);
        });
    }

    private Observable<HttpResponse> getEdges(Id accessKey, Collection<Id> grantKeys) {
        Observable<Collection<Overlord>> overlordSource;
        // FIXME
        // FIXME for now, share overlords between access keys
//        context.adminModel.justOverlords(accessKey);
        overlordSource = context.adminModel.justOverlords();
        return overlordSource.map(overlords -> {
            JsonArray authoritiesArray = new JsonArray();
            overlords.stream().forEach(overlord -> {
                authoritiesArray.add(new JsonPrimitive(overlord.authority.toString()));
            });

            JsonObject returnObject = new JsonObject();
            returnObject.add("authorities", authoritiesArray);

            return NettyUtils.jsonResponse(returnObject);
        });
    }

    private Observable<HttpResponse> postEdges(Id accessKey, Collection<Id> grantKeys, Collection<BadAuthority> badAuthorities) {
        return getEdges(accessKey, grantKeys).doOnSubscribe(() -> {
            if (null != pagerDuty) {
                for (BadAuthority badAuthority : badAuthorities) {
                    Trigger trigger = new Trigger.Builder(String.format("%s reported %s unreachable via DNS",
                            badAuthority.reporter, badAuthority.authority))
                            .withIncidentKey(String.format("%s %s", badAuthority.reporter, badAuthority.authority))
                            .addDetails(ImmutableMap.of(
                                    "reporterIp", "" + badAuthority.reporter,
                                    "authority", "" + badAuthority.authority,
                                    "schemes", badAuthority.schemes.stream().map(Object::toString).collect(Collectors.joining(",")),
                                    "accessKey", "" + accessKey
                            ))
                            .build();
                    pagerDuty.notify(trigger);
                }
            }
        });
    }

    private Observable<HttpResponse> postAccessKey(Id accessKey, Collection<Id> grantKeys) {
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
    }



    /////// MAIN ///////

    public static void main(String[] in) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("dns")
                .defaultHelp(true)
                .description("Nextop dns");
        parser.addArgument("-c", "--configFile")
                .nargs("*")
                .help("JSON config file");
        parser.addArgument("actions")
                .nargs("+")
                .choices("start");

        try {
            Namespace ns = parser.parseArgs(in);
            main(ns);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(400);
        } catch (Exception e) {
            localLog.log(Level.SEVERE, "dns.main", e);
            parser.printUsage();
            System.exit(400);
        }
    }
    private static void main(Namespace ns) throws Exception {
        JsonObject defaultConfigObject = getDefaultConfigObject();

        List<String> configFiles = CliUtils.getList(ns, "configFile");

        DnsService dnsService = new DnsService(defaultConfigObject,
                configFiles.toArray(new String[configFiles.size()]));

        for (String action : CliUtils.<String>getList(ns, "actions")) {
            switch (action) {
                case "start":
                    new ApiContainer(dnsService).start(status -> {
                        localLog.log(Level.INFO, String.format("%-20s %s", "dns.main.init", status));
                    });
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        }
    }
    private static JsonObject getDefaultConfigObject() throws IOException {
        // extract the default (bundled) config object
        Reader r = new BufferedReader(new InputStreamReader(ClassLoader.getSystemClassLoader().getResourceAsStream("local.conf.json"), Charsets.UTF_8));
        try {
            return new JsonParser().parse(r).getAsJsonObject();
        } finally {
            r.close();
        }
    }
}
