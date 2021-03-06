package io.nextop.service.nx;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.squareup.pagerduty.incidents.PagerDuty;
import com.squareup.pagerduty.incidents.Trigger;
import io.nextop.ApiComponent;
import io.nextop.ApiContainer;
import io.nextop.Authority;
import io.nextop.Id;
import io.nextop.service.m.Cloud;
import io.nextop.util.CliUtils;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.http.HttpStatus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class Nx extends ApiComponent.Base {
    private static final Logger localLog = Logger.getGlobal();




    public Nx(JsonObject defaultConfigObject, String ... configFiles) {

    }



    /////// COMMANDS ///////

    /* these must block until completed */

    public void id() {
        System.out.printf("%s\n", Id.create());

//        // FIXME
//        PagerDuty pagerDuty = PagerDuty.create("5907048d5ed243468f197d7fd53a3939");
//        Trigger trigger = new Trigger.Builder(String.format("%s reported %s unreachable via DNS",
//                "223.41.7.60", String.format("%s:%d", "54.149.233.13", 27000)))
//                .withIncidentKey(String.format("%s %s:%d", "223.41.7.60", "54.149.233.13  ", 27000))
//                .addDetails(ImmutableMap.of(
//                        "clientIp", "223.41.7.60",
//                        "authority", String.format("%s:%d", "54.149.233.13", 27000),
//                        "schemes", "27000;17000",
//                        "accessKey", "d0cdc7556841544e78f34a70c48f8067f9cac7689bedf1e5fde6325974a6a888"
//                ))
//                .build();
//        pagerDuty.notify(trigger);
    }

    public void addOverlord(Authority authority, Cloud cloud) {

    }

    public void removeOverlord(Authority authority) {

    }

    public void listOverlords() {

    }

    // TODO init and migrate (upgrade) overlords



    /////// MAIN ///////

    public static void main(String[] in) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("overlord")
                .defaultHelp(true)
                .description("Nextop overlord");
        parser.addArgument("-c", "--configFile")
                .nargs("*")
                .help("JSON config file");
        parser.addArgument("-a", "--accessKey")
                .help("Access key");
        parser.addArgument("-H", "--host")
                .help("Host");
        parser.addArgument("-P", "--port")
                .help("nextop port (http is 10000 lower; https is 5000 lower)");
        parser.addArgument("-d", "--cloud")
                .choices(Arrays.stream(Cloud.values()).map(Object::toString).toArray())
                .help("Cloud");
        parser.addArgument("actions")
                .nargs("+")
                .choices("id", "add", "remove", "ls");

        try {
            Namespace ns = parser.parseArgs(in);
            main(ns);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(HttpStatus.SC_BAD_REQUEST);
        } catch (Exception e) {
            localLog.log(Level.SEVERE, "nx.main", e);
            parser.printUsage();
            System.exit(HttpStatus.SC_INTERNAL_SERVER_ERROR);
        }
    }
    private static void main(Namespace ns) throws Exception {
        JsonObject defaultConfigObject = getDefaultConfigObject();
        List<String> configFiles = CliUtils.getList(ns, "configFile");

        Nx nx = new Nx(defaultConfigObject,
                configFiles.toArray(new String[configFiles.size()]));

        try (ApiContainer c = new ApiContainer(nx)) {
            c.start(status -> {
                localLog.log(Level.INFO, String.format("%-20s %s", "overlord.main.init", status));
            });

            for (String action : CliUtils.<String>getList(ns, "actions")) {
                switch (action) {
                    case "id":
                        nx.id();
                        break;
                    default:
                        throw new IllegalArgumentException();
                }
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
