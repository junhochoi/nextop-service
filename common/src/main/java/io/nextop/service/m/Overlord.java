package io.nextop.service.m;

import io.nextop.Authority;
import io.nextop.Id;

import javax.annotation.Nullable;

public class Overlord {
    public Authority authority;

    public Id localKey;

    @Nullable
    public Cloud cloud = null;

    @Nullable
    public OverlordStatus status = null;
}
