package io.nextop.service.nx;

import com.google.common.base.Charsets;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.nextop.rx.util.ConfigWatcher;
import io.nextop.service.Authority;
import io.nextop.service.NxId;
import io.nextop.service.admin.AdminContext;
import io.nextop.service.admin.AdminController;
import io.nextop.service.admin.AdminModel;
import io.nextop.service.log.ServiceLog;
import io.nextop.service.m.Cloud;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static io.nextop.service.util.ClUtils.*;

public final class Nx {
    private static final ServiceLog log = new ServiceLog();


    private final Scheduler nxScheduler;
    private final Scheduler modelScheduler;
    private final Scheduler controllerScheduler;

    private final ConfigWatcher configWatcher;

    private final AdminContext context;


    Nx(JsonObject defaultConfigObject, String ... configFiles) {
        nxScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
                        new Thread(r, "Nx Worker")
        ));
        modelScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
                        new Thread(r, "AdminModel Worker")
        ));
        controllerScheduler = Schedulers.from(Executors.newFixedThreadPool(4, (Runnable r) ->
                        new Thread(r, "AdminController Worker")
        ));

        configWatcher = new ConfigWatcher(nxScheduler, defaultConfigObject, configFiles);

        context = new AdminContext();
        new AdminModel(context, modelScheduler, configWatcher.getMergedObservable().map(configObject ->
                configObject.get("adminModel").getAsJsonObject()));
        new AdminController(context, controllerScheduler);
    }


    public void start() {
        context.start();
    }

    public void stop() {
        context.stop();
    }


    /////// COMMANDS ///////

    /* these must block until completed */

    public void id() {
        System.out.printf("%s\n", NxId.create());
    }

    public void addOverlord(Authority authority, Cloud cloud) {

    }

    public void removeOverlord(Authority authority) {

    }

    public void listOverlords() {

    }

    public void init(NxId accessKey, String packageTag) {

    }

    public void migrate(NxId accessKey, String packageTag) {

    }



    /////// MAIN ///////

    public static void main(String[] in) {
        Options options = new Options();
        options.addOption("c", "configFile", true, "JSON config file");
        options.addOption("h", "host", true, "Public host (dot notation)");
        options.addOption("p", "port", true, "Port");
        options.addOption("d", "cloud", true, "Cloud (aws, gce, ma)");
        options.addOption("a", "accessKey", true, "Access key");
        options.addOption("a", "accessKey", true, "Access key");

        CommandLine cl;
        try {
            main(new GnuParser().parse(options, in));
            // force an exit because schedulers are not fully daemonized
            // @see https://github.com/ReactiveX/RxJava/issues/1730
            System.exit(0);
        } catch (Exception e) {
            new HelpFormatter().printHelp("nx [id] [add] [remove] [ls] [init] [migrate]",
                    options);
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

        // options
        String[] configFiles = getStrings(cl, 'c', new String[0]);
        @Nullable String publicHost = getString(cl, 'h', null);
        int port = getInt(cl, 'p', -1);
        @Nullable Cloud cloud = getParsed(cl, 'd', name -> Cloud.valueOf(name.toLowerCase()), null);

        Nx nx = new Nx(defaultConfigObject, configFiles);
        nx.start();
        try {
            Stream.of(cl.getArgs()).map(String::toLowerCase).forEach(arg -> {
                switch (arg) {
                    case "id":
                        nx.id();
                        break;
                    case "add":
                        if (null == publicHost) {
                            throw new IllegalArgumentException();
                        }
                        if (port <= 0) {
                            throw new IllegalArgumentException();
                        }
                        if (null == cloud) {
                            throw new IllegalArgumentException();
                        }
                        nx.addOverlord(new Authority(publicHost, port), cloud);
                        break;
                    case "remove":
                        if (null == publicHost) {
                            throw new IllegalArgumentException();
                        }
                        if (port <= 0) {
                            throw new IllegalArgumentException();
                        }
                        nx.removeOverlord(new Authority(publicHost, port));
                        break;
                    case "ls":
                        nx.listOverlords();
                        break;
                    default:
                        throw new IllegalArgumentException();
                }
            });
        } finally {
            nx.stop();
        }
    }
}
