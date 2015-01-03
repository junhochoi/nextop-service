package io.nextop.service.overlord;

import com.google.common.base.Charsets;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.handler.codec.http.*;
import io.nextop.rx.http.BasicRouter;
import io.nextop.rx.http.NettyServer;
import io.nextop.rx.http.Router;
import io.nextop.rx.util.ConfigWatcher;
import io.nextop.service.NxId;
import io.nextop.service.Permission;
import io.nextop.service.log.ServiceLog;
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
import java.util.stream.Stream;

// FIXME figure out how to package schema upgades into docker
// FIXME run upgrades when docker starts (dns, hyperlord, overlord, etc)
public class OverlordService {
    private static final ServiceLog log = new ServiceLog();

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


    private final Scheduler overlordScheduler;
    private final Scheduler modelScheduler;

    private final ConfigWatcher configWatcher;
    private final NettyServer httpServer;

    private final OverlordContext context;

    private final Observable<NxId> accessKeySource;


    OverlordService(JsonObject defaultConfigObject, String ... configFiles) {
        overlordScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
                        new Thread(r, "OverlordService Worker")
        ));
        modelScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
                        new Thread(r, "OverlordModel Worker")
        ));

        configWatcher = new ConfigWatcher(overlordScheduler, defaultConfigObject, configFiles);
        httpServer = new NettyServer(overlordScheduler, router());

        accessKeySource = configWatcher.getMergedObservable().flatMap(configObject -> {
            try {
                return Observable.just(NxId.valueOf(configObject.get("accessKey").getAsString()));
            } catch (Exception e) {
                return Observable.<NxId>empty();
            }
        });

        // CONTEXT
        context = new OverlordContext();
        // FIXME
//        new OverlordModel(context, modelScheduler, configWatcher.getMergedObservable().map(configObject ->
//                configObject.get("overlordModel").getAsJsonObject()));
    }


    public void start()  {
        configWatcher.start();


        httpServer.start(configWatcher.getMergedObservable().map(configObject -> {
            NettyServer.Config config = new NettyServer.Config();
            config.httpPort = configObject.get("httpPort").getAsInt();
            return config;
        }));


        // FIXME start monitoring up overlords
//        context.adminController.startMonitor()
    }

    public void stop() {
        httpServer.stop();
        configWatcher.stop();
    }


    /////// ROUTES ///////

    private Observable<HttpResponse> getMetrics() {
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
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
        // FIXME command line option for access key

        CommandLine cl;
        try {
            main(new GnuParser().parse(options, in));
        } catch (Exception e) {
            log.unhandled("overlord.main", e);
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
                    overlordService.start();
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        });
    }
}
