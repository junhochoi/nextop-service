package io.nextop.service;

import com.google.gson.JsonObject;
import rx.Observable;
import rx.Scheduler;

import java.sql.Connection;
import java.util.Collection;

/** Data access for the service */
// TODO verify that Observables return SafeSubscriber subscriptions
public class ServiceData {
    private final Scheduler scheduler;

    private final Observable<Connection> connectionSource;


    public ServiceData(Scheduler scheduler, Observable<JsonObject> configSource) throws Exception {
        // FIXME parse config file, set up DB connection
    }




    public <T> Observable<T> requirePermissions(Observable<T> source, NxId accessKey, Iterable<NxId> grantKeys, Permission.Mask ... permissionMasks) {

    }




    // all "just" API methods do an action then call onComplete or onError.




    public Observable<Authority> justReserveOverlord() {

    }

    public Observable<ApiResult> justReleaseOverlord(Authority a) {

    }

    // FIXME overlord status


    public Observable<ApiResult> justGrant(NxId accessKey, NxId grantKey, Permission.Mask ... permissionMasks) {

    }







    public Observable<ApiResult> justDirtyPermissions(NxId accessKey) {
        // FIXME dirty shared cache
    }

    // returns up overlords
    public Observable<Collection<Authority>> justOverlords(NxId accessKey, boolean monitorUp) {

    }

    public Observable<ApiResult> justDirtyOverlords(NxId accessKey) {
        //
        // FIXME dirty shared cache

        // FIXME update CloudFront
    }

}
