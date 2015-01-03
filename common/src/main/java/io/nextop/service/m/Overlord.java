package io.nextop.service.m;

import io.nextop.service.Authority;
import io.nextop.service.NxId;

import javax.annotation.Nullable;

public class Overlord {
    public Authority authority;

    public NxId localKey;

    @Nullable
    public Cloud cloud = null;

    @Nullable
    public OverlordStatus status = null;
}
