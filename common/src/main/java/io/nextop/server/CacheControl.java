package io.nextop.server;

import io.nextop.Message;

// FIXME interpretation of Cache-Control headers
public final class CacheControl {

    public static CacheControl get(Message message) {
        // FIXME parse
        return new CacheControl(StorageLevel.GLOBAL);
    }


    public static enum StorageLevel {
        GLOBAL,
        PRIVATE,
        NONE
    }



    public final StorageLevel storageLevel;


    CacheControl(StorageLevel storageLevel) {
        this.storageLevel = storageLevel;
    }


    public boolean isCacheable() {
        return !StorageLevel.NONE.equals(storageLevel);
    }
}
