package io.nextop.util;

import org.apache.commons.cli.CommandLine;

import javax.annotation.Nullable;
import java.util.function.Function;

public class ClUtils {

    @Nullable
    public static String getString(CommandLine cl, char opt, @Nullable String defaultValue) {
        @Nullable String value = cl.getOptionValue(opt);
        return null != value ? value : defaultValue;
    }

    @Nullable
    public static String[] getStrings(CommandLine cl, char opt, @Nullable String[] defaultValues) {
        @Nullable String[] values = cl.getOptionValues(opt);
        return null != values ? values : defaultValues;
    }

    public static int getInt(CommandLine cl, char opt, int defaultValue) {
        @Nullable String value = cl.getOptionValue(opt);
        if (null != value) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                // fall through
            }
        }
        return defaultValue;
    }

    @Nullable
    public static <R> R getParsed(CommandLine cl, char opt, Function<String, R> parser, R defaultValue) {
        @Nullable String value = cl.getOptionValue(opt);
        if (null != value) {
            try {
                return parser.apply(value);
            } catch (Exception e) {
                // fall through
            }
        }
        return defaultValue;
    }


    private ClUtils() {
    }
}
