package io.nextop.service.overlord;

import com.google.common.base.Charsets;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.nextop.ApiComponent;
import io.nextop.ApiContainer;
import io.nextop.db.DataSourceProvider;
import io.nextop.http.BasicRouter;
import io.nextop.http.NettyHttpServer;
import io.nextop.http.Router;
import io.nextop.rx.MoreRxOperations;
import io.nextop.service.admin.AdminContext;
import io.nextop.service.admin.AdminModel;
import io.nextop.util.ConfigWatcher;
import io.nextop.service.NxId;
import io.nextop.service.Permission;
import io.nextop.service.log.ServiceLog;
import io.nextop.util.NettyUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

// FIXME figure out how to package schema upgades into docker
// FIXME run upgrades when docker starts (dns, hyperlord, overlord, etc)
public class OverlordService extends ApiComponent.Base {
    private static final Logger localLog = Logger.getGlobal();


    private final Router router() {
        BasicRouter router = new BasicRouter();
        Object grantKeyMatcher = BasicRouter.var("grant-key", segment -> NxId.valueOf(segment));

        Function<Map<String, List<?>>, Map<String, List<?>>> validate = parameters -> {
            return parameters;
        };
        Function<Map<String, List<?>>, Map<String, List<?>>> validatePermissionMasks = parameters -> {
            // $permission-name=$permission-value
            Permission[] permissions = Permission.values();
            List<Permission.Mask> masks = new ArrayList<>(permissions.length);
            for (Permission permission : permissions) {
                List<Object> values = (List<Object>) parameters.getOrDefault(permission.name(), Collections.emptyList());
                if (!values.isEmpty()) {
                    masks.add(permission.mask(Boolean.parseBoolean(values.get(0).toString())));
                }
            }
            parameters.put("permission-mask", masks);

            return parameters;
        };
        Function<Map<String, List<?>>, Map<String, List<?>>> validateConfigMask = parameters -> {
            FullHttpRequest request = (FullHttpRequest) parameters.get("request").get(0);
            JsonObject configMaskObject = new JsonParser().parse(new InputStreamReader(
                    new ByteArrayInputStream(request.content().array()), Charsets.UTF_8)).getAsJsonObject();
            parameters.put("config-mask", Collections.singletonList(configMaskObject));

            return parameters;
        };

        router.add(HttpMethod.GET, Arrays.asList("metrics"), validate.andThen(parameters ->
                getMetrics()));
        router.add(HttpMethod.PUT, Arrays.asList("grant-key", grantKeyMatcher), validate.andThen(validatePermissionMasks
        ).andThen(parameters -> {
            NxId grantKey = (NxId) parameters.get("grant-key").get(0);
            List<Permission.Mask> masks = (List<Permission.Mask>) parameters.get("permission-mask");
            return putGrantKey(grantKey, masks);
        }));
        router.add(HttpMethod.POST, Arrays.asList("grant-key", grantKeyMatcher), validate.andThen(validatePermissionMasks
        ).andThen(parameters -> {
            NxId grantKey = (NxId) parameters.get("grant-key").get(0);
            List<Permission.Mask> masks = (List<Permission.Mask>) parameters.get("permission-mask");
            return postGrantKey(grantKey, masks);
        }));
        router.add(HttpMethod.DELETE, Arrays.asList("grant-key", grantKeyMatcher), validate.andThen(parameters -> {
            NxId grantKey = (NxId) parameters.get("grant-key").get(0);
            return deleteGrantKey(grantKey);
        }));
        router.add(HttpMethod.GET, Arrays.asList("config"), validate.andThen(parameters -> {
            return getConfig();
        }));
        router.add(HttpMethod.POST, Arrays.asList("config"), validate.andThen(validateConfigMask
        ).andThen(parameters -> {
            JsonObject configMaskObject = (JsonObject) parameters.get("config-mask").get(0);
            return postConfig(configMaskObject);
        }));

        return router;
    }


    private final Scheduler apiScheduler;
    private final Scheduler modelScheduler;

    private final ConfigWatcher configWatcher;
    private final NettyHttpServer httpServer;

    private final OverlordMetrics metrics;




    OverlordService(JsonObject defaultConfigObject, String ... configFiles) {
        apiScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
                        new Thread(r, "OverlordService Worker")
        ));
        modelScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
                        new Thread(r, "OverlordModel Worker")
        ));

        configWatcher = new ConfigWatcher(modelScheduler, defaultConfigObject, configFiles);
        httpServer = new NettyHttpServer(apiScheduler, router(),
                configWatcher.getMergedObservable().map(configObject -> {
                    JsonObject httpConfigObject = configObject.get("http").getAsJsonObject();

                    // FIXME fix the parsing here
                    NettyHttpServer.Config config = new NettyHttpServer.Config();
                    config.httpPort = httpConfigObject.get("port").getAsInt();
                    return config;
                }));

        metrics = new OverlordMetrics();

        init = ApiComponent.layerInit(configWatcher.init(),
                httpServer.init());
    }



    /////// ROUTES ///////

    private Observable<HttpResponse> getMetrics() {
        return Observable.just(NettyUtils.jsonResponse(metrics.getSnapshot().toJson()));
    }

    private Observable<HttpResponse> putGrantKey(NxId grantKey, List<Permission.Mask> masks) {
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
    }

    private Observable<HttpResponse> postGrantKey(NxId grantKey, List<Permission.Mask> masks) {
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
    }

    private Observable<HttpResponse> deleteGrantKey(NxId grantKey) {
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
    }

    private Observable<HttpResponse> getConfig() {
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
    }

    private Observable<HttpResponse> postConfig(JsonObject configObjectMask) {
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
    }


    /////// MAIN ///////

    public static void main(String[] in) {
        Options options = new Options();
        options.addOption("c", "configFile", true, "JSON config file");
        options.addOption("a", "accessKey", true, "Access key");
        options.addOption("P", "port", true, "Port");

        // FIXME merge the access key and port into the default config

        // FIXME command line option for access key

        CommandLine cl;
        try {
            main(new GnuParser().parse(options, in));
        } catch (Exception e) {
            // FIXME log
//            lopcal.unhandled("overlord.main", e);
            new HelpFormatter().printHelp("overlord", options);
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

        OverlordService overlordService = new OverlordService(defaultConfigObject, null != configFiles ? configFiles : new String[0]);

        Stream.of(cl.getArgs()).map(String::toLowerCase).forEach(arg -> {
            switch (arg) {
                case "start":
                    new ApiContainer(overlordService).start(status -> {
                        localLog.log(Level.INFO, String.format("%-20s %s", "dns.main.init", status));
                    });
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        });
    }
}
