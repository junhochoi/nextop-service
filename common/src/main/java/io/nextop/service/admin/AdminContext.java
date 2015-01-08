package io.nextop.service.admin;

import io.nextop.ApiComponent;
import io.nextop.db.DataSourceProvider;
import io.nextop.service.log.ServiceLog;
import rx.Scheduler;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public final class AdminContext implements ApiComponent {

    public Scheduler scheduler;

    @Nullable
    public ServiceLog log = null;

    @Nullable
    public DataSourceProvider dataSourceProvider = null;

    @Nullable
    public AdminModel adminModel = null;

    @Nullable
    public AdminController adminController = null;


    @Override
    public Init init() {
        List<Init> inits = new ArrayList<Init>(4);
        if (null != log) {
            inits.add(log.init());
        }
        if (null != dataSourceProvider) {
            inits.add(dataSourceProvider.init());
        }
        if (null != adminModel) {
            inits.add(adminModel.init());
        }
        if (null != adminController) {
            inits.add(adminController.init());
        }
        return ApiComponent.layerInit(inits);
    }
}
