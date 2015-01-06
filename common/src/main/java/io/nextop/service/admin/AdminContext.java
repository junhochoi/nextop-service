package io.nextop.service.admin;

import io.nextop.ApiComponent;
import io.nextop.ApiStatus;
import io.nextop.service.log.ServiceLog;
import rx.Observable;

import javax.annotation.Nullable;

public final class AdminContext implements ApiComponent {

    @Nullable
    public ServiceLog log = null;

    @Nullable
    public AdminModel adminModel = null;

    @Nullable
    public AdminController adminController = null;


    @Override
    public Observable<ApiStatus> init() {
        Observable<ApiStatus> init = Observable.empty();
        if (null != log) {
            // FIXME layer
            init = Observable.concat(init, log.init());
        }
        if (null != adminModel) {
            // FIXME layer
            init = Observable.concat(init, adminModel.init());
        }
        if (null != adminController) {
            // FIXME layer
            init = Observable.concat(init, adminController.init());
        }
        return init;
    }
}
