package io.nextop.service.admin;

import io.nextop.service.log.ServiceLog;

import javax.annotation.Nullable;

public final class AdminContext {

    @Nullable
    public ServiceLog log = null;

    @Nullable
    public AdminModel adminModel = null;

    @Nullable
    public AdminController adminController = null;
}
