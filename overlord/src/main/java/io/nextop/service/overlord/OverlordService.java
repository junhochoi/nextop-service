package io.nextop.service.overlord;

import com.google.common.base.Charsets;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.handler.codec.http.*;
import io.nextop.ApiComponent;
import io.nextop.ApiContainer;
import io.nextop.http.BasicRouter;
import io.nextop.http.NettyHttpServer;
import io.nextop.http.Router;
import io.nextop.Id;
import io.nextop.service.Permission;
import io.nextop.util.CliUtils;
import io.nextop.util.ConfigWatcher;
import io.nextop.util.NettyUtils;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.http.HttpStatus;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.io.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

// FIXME figure out how to package schema upgades into docker
// FIXME run upgrades when docker starts (dns, hyperlord, overlord, etc)
public class OverlordService extends ApiComponent.Base {
    private static final Logger localLog = Logger.getGlobal();


    private final Router router() {
        BasicRouter router = new BasicRouter();
        Object grantKeyMatcher = BasicRouter.var("grant-key", segment -> Id.valueOf(segment));

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
            FullHttpRequest request = (FullHttpRequest) parameters.get(NettyHttpServer.P_REQUEST).get(0);
            JsonObject configMaskObject = new JsonParser().parse(request.content().toString(Charsets.UTF_8)).getAsJsonObject();
            parameters.put("config-mask", Collections.singletonList(configMaskObject));

            return parameters;
        };

        router.add(HttpMethod.GET, Arrays.asList("metrics.json"), validate.andThen(parameters ->
                getMetrics()));
        router.add(HttpMethod.PUT, Arrays.asList("grant-key", grantKeyMatcher), validate.andThen(validatePermissionMasks
        ).andThen(parameters -> {
            Id grantKey = (Id) parameters.get("grant-key").get(0);
            List<Permission.Mask> masks = (List<Permission.Mask>) parameters.get("permission-mask");
            return putGrantKey(grantKey, masks);
        }));
        router.add(HttpMethod.POST, Arrays.asList("grant-key", grantKeyMatcher), validate.andThen(validatePermissionMasks
        ).andThen(parameters -> {
            Id grantKey = (Id) parameters.get("grant-key").get(0);
            List<Permission.Mask> masks = (List<Permission.Mask>) parameters.get("permission-mask");
            return postGrantKey(grantKey, masks);
        }));
        router.add(HttpMethod.DELETE, Arrays.asList("grant-key", grantKeyMatcher), validate.andThen(parameters -> {
            Id grantKey = (Id) parameters.get("grant-key").get(0);
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

    private Observable<HttpResponse> putGrantKey(Id grantKey, List<Permission.Mask> masks) {
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
    }

    private Observable<HttpResponse> postGrantKey(Id grantKey, List<Permission.Mask> masks) {
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
    }

    private Observable<HttpResponse> deleteGrantKey(Id grantKey) {
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
        ArgumentParser parser = ArgumentParsers.newArgumentParser("overlord")
                .defaultHelp(true)
                .description("Nextop overlord");
        parser.addArgument("-c", "--configFile")
                .nargs("*")
                .help("JSON config file");
        parser.addArgument("-a", "--accessKey")
                .required(true)
                .help("Access key");
        parser.addArgument("-P", "--port")
                .required(true)
                .type(int.class)
                .help("nextop port (http is 10000 lower; https is 5000 lower)");
        parser.addArgument("actions")
                .nargs("+")
                .choices("start");

        try {
            Namespace ns = parser.parseArgs(in);
            main(ns);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(HttpStatus.SC_BAD_REQUEST);
        } catch (Exception e) {
            localLog.log(Level.SEVERE, "overlord.main", e);
            parser.printUsage();
            System.exit(HttpStatus.SC_INTERNAL_SERVER_ERROR);
        }
    }
    private static void main(Namespace ns) throws Exception {
        JsonObject defaultConfigObject = new JsonObject();
        ConfigWatcher.mergeDown(defaultConfigObject,
                getDefaultConfigObject(), createArgConfigObject(ns));

        List<String> configFiles = CliUtils.getList(ns, "configFile");

        OverlordService overlordService = new OverlordService(defaultConfigObject,
                configFiles.toArray(new String[configFiles.size()]));

        for (String action : CliUtils.<String>getList(ns, "actions")) {
            switch (action) {
                case "start":
                    new ApiContainer(overlordService).start(status -> {
                        localLog.log(Level.INFO, String.format("%-20s %s", "overlord.main.init", status));
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
    private static JsonObject createArgConfigObject(Namespace ns) {
        Id accessKey = Id.valueOf(ns.getString("accessKey"));
        int port = ns.getInt("port");

        JsonObject nextopObject = new JsonObject();
        nextopObject.addProperty("port", port);
        JsonObject httpObject = new JsonObject();
        httpObject.addProperty("port", port - 10000);

        JsonObject baseConfigObject = new JsonObject();
        baseConfigObject.addProperty("accessKey", accessKey.toString());
        baseConfigObject.add("nextop", nextopObject);
        baseConfigObject.add("http", httpObject);

        return baseConfigObject;
    }
}
