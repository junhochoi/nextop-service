package io.nextop.service.overlord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// FIXME re-use admin model but use a sqlite db
// FIXME figure out how to package schema upgades into docker
// FIXME run upgrades when docker starts (dns, hyperlord, overlord, etc)
public class OverlordService {
    private final Router router() {
        BasicRouter router = new BasicRouter();
        Object accessKeyMatcher = BasicRouter.var("access-key", segment -> NxId.valueOf(segment));
        Object grantKeyMatcher = BasicRouter.var("grant-key", segment -> NxId.valueOf(segment));

        Function<Map<String, List<?>>, Map<String, List<?>>> validate = parameters -> {
            return parameters;
        };
        Function<Map<String, List<?>>, Map<String, List<?>>> validateGitCommitHash = parameters -> {
            parameters.put("git-commit-hash", parameters.getOrDefault("git-commit-hash", Collections.emptyList()));
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

        // GET https://$access-key.nextop.io/metrics
        // GET https://$access-key.nextop.io/config
        // POST https://$access-key.nextop.io/config


        router.add(HttpMethod.PUT, Arrays.asList(accessKeyMatcher), validate.andThen(validateGitCommitHash
        ).andThen(parameters -> {
            parameters.put("root-grant-key", ((List<String>) parameters.getOrDefault("root-grant-key", Collections.emptyList()
            )).stream().map((String grantKeyString) -> NxId.valueOf(grantKeyString)).collect(Collectors.toList()));
            return parameters;
        }).andThen(parameters -> {
            NxId accessKey = (NxId) parameters.get("access-key").get(0);
            NxId rootGrantKey = ((List<NxId>) parameters.get("root-grant-key")).get(0);
            String gitCommitHash = parameters.get("git-commit-hash").get(0).toString();
            return putAccessKey(accessKey, rootGrantKey, gitCommitHash);
        }));
        router.add(HttpMethod.DELETE, Arrays.asList(accessKeyMatcher), validate.andThen(parameters -> {
            NxId accessKey = (NxId) parameters.get("access-key").get(0);
            return deleteAccessKey(accessKey);
        }));
        router.add(HttpMethod.GET, Arrays.asList(accessKeyMatcher, "grant-key"), validate.andThen(parameters -> {
            NxId accessKey = (NxId) parameters.get("access-key").get(0);
            return getGrantKeys(accessKey);
        }));
        router.add(HttpMethod.PUT, Arrays.asList(accessKeyMatcher, "grant-key", grantKeyMatcher), validate.andThen(validatePermissionMasks
        ).andThen(parameters -> {
            NxId accessKey = (NxId) parameters.get("access-key").get(0);
            NxId grantKey = (NxId) parameters.get("grant-key").get(0);
            List<Permission.Mask> masks = (List<Permission.Mask>) parameters.get("permission-mask");
            return putGrantKey(accessKey, grantKey, masks);
        }));
        router.add(HttpMethod.POST, Arrays.asList(accessKeyMatcher, "grant-key", grantKeyMatcher), validate.andThen(validatePermissionMasks
        ).andThen(parameters -> {
            NxId accessKey = (NxId) parameters.get("access-key").get(0);
            NxId grantKey = (NxId) parameters.get("grant-key").get(0);
            List<Permission.Mask> masks = (List<Permission.Mask>) parameters.get("permission-mask");
            return updateGrantKey(accessKey, grantKey, masks);
        }));
        router.add(HttpMethod.POST, Arrays.asList(accessKeyMatcher, "grant-key", grantKeyMatcher), validate.andThen(parameters -> {
            NxId accessKey = (NxId) parameters.get("access-key").get(0);
            NxId grantKey = (NxId) parameters.get("grant-key").get(0);
            return deleteGrantKey(accessKey, grantKey);
        }));


        return router;
    }


    private final Scheduler hyperlordScheduler;
    private final Scheduler modelScheduler;
    private final Scheduler controllerScheduler;

    private final ConfigWatcher configWatcher;
    private final NettyServer httpServer;

    private final ServiceContext context;


    HyperlordService(JsonObject defaultConfigObject, String ... configFiles) {
        hyperlordScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
                        new Thread(r, "HyperlordService Worker")
        ));
        modelScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
                        new Thread(r, "ServiceAdminModel Worker")
        ));
        controllerScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
                        new Thread(r, "ServiceAdminController Worker")
        ));

        configWatcher = new ConfigWatcher(hyperlordScheduler, defaultConfigObject, configFiles);
        httpServer = new NettyServer(hyperlordScheduler, router());

        // CONTEXT
        context = new ServiceContext();
        new ServiceAdminModel(context, modelScheduler, configWatcher.getMergedObservable().map(configObject ->
                configObject.get("adminModel").getAsJsonObject()));
        new ServiceAdminController(context, controllerScheduler);


    }


    public void start()  {

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
    }


    /////// ROUTES ///////

    private Observable<HttpResponse> putAccessKey(NxId accessKey, NxId rootGrantKey, String gitCommitHash) {
//        context.adminController.initAccessKey(accessKey, rootGrantKey, gitCommitHash);
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
    }

    private Observable<HttpResponse> deleteAccessKey(NxId accessKey) {
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
    }

    private Observable<HttpResponse> getGrantKeys(NxId accessKey) {
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
    }

    private Observable<HttpResponse> putGrantKey(NxId accessKey, NxId grantKey, List<Permission.Mask> masks) {
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
    }

    private Observable<HttpResponse> updateGrantKey(NxId accessKey, NxId grantKey, List<Permission.Mask> masks) {
        // FIXME
        return Observable.just(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
    }

    private Observable<HttpResponse> deleteGrantKey(NxId accessKey, NxId grantKey) {
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
            log.unhandled("hyperlord.main", e);
            new HelpFormatter().printHelp("hyperlord", options);
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
                HyperlordService hyperlordService = new HyperlordService(defaultConfigObject, null != configFiles ? configFiles : new String[0]);
                hyperlordService.start();
            } else {
                throw new IllegalArgumentException();
            }
        });
    }
}
