package io.nextop.service.m;

import javax.annotation.Nullable;

public class OverlordStatus {
    @Nullable
    public String gitCommitHash = null;

    @Nullable
    public String deepMd5 = null;

    public boolean monitorUp = false;
}
