package io.nextop.service.admin;

import io.nextop.ApiComponent;
import io.nextop.ApiStatus;
import io.nextop.Id;
import rx.Observable;
import rx.Scheduler;

public class AdminController extends ApiComponent.Base {
    // FIXME connections to overlords

    private final AdminContext context;
    private final Scheduler scheduler;


    public AdminController(AdminContext context, Scheduler scheduler /* FIXME config */) {
        this.context = context;
        assert null == context.adminController;
        context.adminController = this;

        this.scheduler = scheduler;
    }


    /** set up an access key with no existing overlords. Assigns admin permissions to the given
     * grant key and instantiates an overlord process with the given code hash on a free host. */
    public Observable<ApiStatus> initAccessKey(Id accessKey, Id rootGrantKey, String packageTag) {
        // FIXME
        return null;
    }

    // transition the existing up overlords to new instances on the code hash.
    // attempt to match the cloud and regions from old to new.
    // FIXME in model mark overlords that are scheduled to be taken down
    // FIXME once new overlord is up, send a message to existing overlord to shut down and
    // FIXME migrate its connections to its closest pair
    public Observable<ApiStatus> migrateAccessKey(Id accessKey, String gitCommitHash) {
        // FIXME
        return null;
    }



    // FIXME private observable to start an overlord with git commit hash and monitor its status
    // FIXME private observable to shut down an overlord and merge its connections into another overlord


    // FIXME scaler for an access key, ensures overlords for the access key are active


}
