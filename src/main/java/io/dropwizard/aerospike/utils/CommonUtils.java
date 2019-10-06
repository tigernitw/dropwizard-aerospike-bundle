package io.dropwizard.aerospike.utils;

import java.util.Collection;

public interface CommonUtils {

    static <T extends Collection> boolean isEmpty(T collection) {
        return null == collection || collection.isEmpty();
    }
}
