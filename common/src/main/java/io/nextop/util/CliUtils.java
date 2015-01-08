package io.nextop.util;

import net.sourceforge.argparse4j.inf.Namespace;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class CliUtils {

    public static <T> List<T> getList(Namespace ns, String dest) {
        @Nullable List<T> list = ns.<T>getList(dest);
        return null != list ? list : Collections.<T>emptyList();
    }
}
