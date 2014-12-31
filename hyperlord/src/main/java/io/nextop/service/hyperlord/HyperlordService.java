package io.nextop.service.hyperlord;


import com.google.common.base.Charsets;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.nextop.rx.http.BasicRouter;
import io.nextop.rx.http.NettyServer;
import io.nextop.rx.http.Router;
import io.nextop.rx.util.ConfigWatcher;
import io.nextop.service.NxId;
import io.nextop.service.Permission;
import io.nextop.service.ServiceAdminModel;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.nextop.service.log.ServiceLog.log;

public class HyperlordService {
    private final Router router() {
        BasicRouter router = new BasicRouter();
        Object accessKeyMatcher = BasicRouter.var("access-key", segment -> NxId.valueOf(segment));

        Function<Map<String, List<?>>, Map<String, List<?>>> validate = parameters -> {
            parameters.put("grant-key", ((List<String>) parameters.getOrDefault("grant-key", Collections.<String>emptyList()
            )).stream().map((String grantKeyString) -> NxId.valueOf(grantKeyString)).collect(Collectors.toList()));
            return parameters;
        };

        // PUT https://hyperlord.nextop.io/$access-key?set-root-grant-key=$root-grant-key
        // DELETE https://hyperlord.nextop.io/$access-key
        // PUT https://$access-key.nextop.io/grant-key/$grant-key?$permission-name=$permission-value
        // POST https://$access-key.nextop.io/grant-key/$grant-key?$permission-name=$permission-value
        // DELETE https://$access-key.nextop.io/grant-key/$grant-key
        // FIXME

        return router;
    }


    private final Scheduler hyperlordScheduler;
    private final Scheduler modelScheduler;

    private final ConfigWatcher configWatcher;
    private final ServiceAdminModel serviceAdminModel;

    private final NettyServer httpServer;


    HyperlordService(JsonObject defaultConfigObject, String ... configFiles) {
        hyperlordScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
                        new Thread(r, "HyperlordService Worker")
        ));
        modelScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
                        new Thread(r, "ServiceAdminModel Worker")
        ));

        configWatcher = new ConfigWatcher(hyperlordScheduler, defaultConfigObject, configFiles);
        serviceAdminModel = new ServiceAdminModel(modelScheduler, configWatcher.getMergedObservable().map(configObject ->
                configObject.get("adminModel").getAsJsonObject()));

        httpServer = new NettyServer(hyperlordScheduler, router());
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

    // FIXME


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
