package io.nextop.service;

import com.google.gson.JsonObject;
import io.nextop.service.m.Overlord;
import org.apache.http.client.utils.URIBuilder;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.Subscription;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/** Data access for the service */
// TODO verify that Observables return SafeSubscriber subscriptions
public class ServiceData implements AutoCloseable {
    private final Scheduler scheduler;

    private final Observable<JsonObject> configSource;
    private final Observable<Connection> connectionSource;

    // CONFIG

    private volatile int updateTryCount = 1;

    // SUBSCRIPTIONS

    private Subscription configSubscription;


    public ServiceData(Scheduler scheduler, Observable<JsonObject> configSource) throws Exception {
        this.scheduler = scheduler;
        this.configSource = configSource;

        configSubscription = configSource.subscribe(
                (JsonObject configObject) -> {
                    updateTryCount = configObject.get("service").getAsJsonObject().get("updateTryCount").getAsInt();
                },
                (Throwable t) -> {},
                () -> {}
        );


        // FIXME pool this
        // FIXME is there a way to implement the pool in RX? seems possible, make a util
        connectionSource = configSource.observeOn(scheduler
        ).map((JsonObject configObject) -> {
            String host = configObject.get("mysqlHost").getAsString();
            int port = configObject.get("mysqlPort").getAsInt();;
            String db = configObject.get("mysqlDb").getAsString();
            String user = configObject.get("user").getAsString();
            String password = configObject.get("password").getAsString();
            URI mysqlUri;
            try {
                mysqlUri = new URIBuilder().setScheme("jdbc:mysql"
                ).setHost(host
                ).setPort(port
                ).setPath("/" + db
                ).addParameter("user", user
                ).addParameter("password", password
                ).build();
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }

            try {
                Connection connection = DriverManager.getConnection(mysqlUri.toString());

                // config defaults
                connection.setAutoCommit(true);
                connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

                return connection;
            } catch (SQLException e) {
                throw new IllegalArgumentException(e);
            }
        }).doOnEach(new Observer<Connection>() {
            @Nullable
            Connection c = null;

            @Override
            public void onNext(Connection connection) {
                close();
                assert null == c;
                c = connection;
            }
            @Override
            public void onCompleted() {
                close();
            }
            @Override
            public void onError(Throwable e) {
                close();
            }

            void close() {
                // FIXME log close
                if (null != c) {
                    try {
                        c.close();
                    } catch (SQLException e) {
                        // FIXME log
                    }
                    c = null;
                }
            }
        });
    }


    /////// AutoCloseable IMPLEMENTATION ///////

    @Override
    public void close() throws Exception {
        configSubscription.unsubscribe();
    }


    /////// PERMISSIONS ///////

    public <T> Observable<T> requirePermissions(final Observable<T> source, final NxId accessKey, final Collection<NxId> grantKeys, final Permission.Mask ... permissionMasks) {
        return connectionSource.map((Connection connection) -> {
            try {
                // from all given grant keys, gather all permission names with value=true
                // then test if the given masks apply
                final PreparedStatement selectPermissionNames = connection.prepareStatement("SELECT GrantKeyPermission.permission_name" +
                        " FROM GrantKey INNER JOIN GrantKeyPermission ON GrantKey.grant_key = GrantKeyPermission.grant_key" +
                        " WHERE GrantKey.access_key = ? AND GrantKey.grant_key = ? AND GrantKeyPermission.permission_value = true");
                try {
                    Set<Permission> grantKeysPermissions = grantKeys.stream().flatMap(grantKey -> {
                        try {
                            selectPermissionNames.setString(1, accessKey.toString());
                            selectPermissionNames.setString(2, grantKey.toString());
                            ResultSet rs = selectPermissionNames.executeQuery();
                            try {
                                assert "permission_name".equals(rs.getMetaData().getColumnName(1));

                                List<Permission> grantKeyPermissions = new ArrayList<Permission>(4);
                                while (rs.next()) {
                                    grantKeyPermissions.add(Permission.valueOf(rs.getString(1)));
                                }
                                return grantKeyPermissions.stream();
                            } finally {
                                rs.close();
                            }
                        } catch (SQLException e) {
                            throw new ApiException(e);
                        }
                    }).collect(Collectors.toSet());

                    for (Permission.Mask permissionMask : permissionMasks) {
                        if (permissionMask.mask != grantKeysPermissions.contains(permissionMask.p)) {
                            return false;
                        }
                    }
                    return true;
                } finally {
                    selectPermissionNames.close();
                }
            } catch (SQLException e) {
                throw new ApiException(e);
            }
        }).flatMap((boolean authorized) -> {
            if (authorized) {
                return source;
            } else {
                return Observable.error(new ApiException(ApiResult.notAuthorized()));
            }
        });
    }


    /////// API ///////


    // all "just" API methods do an action then call onComplete or onError.




    public Observable<Overlord> justReserveOverlord(final NxId accessKey) {
        return connectionSource.map((Connection connection) -> {
            NxId localKey = NxId.create();
            try {
                for (int i = 0; i < updateTryCount; ++i) {
                    try {
                        connection.setAutoCommit(false);
                        try {
                            PreparedStatement updateOverlord = connection.prepareStatement("UPDATE Overlord SET access_key = ?, local_key = ?" +
                                    " WHERE access_key IS NULL" +
                                    " LIMIT 1");
                            try {
                                updateOverlord.setString(1, accessKey.toString());
                                updateOverlord.setString(2, localKey.toString());

                                int c = updateOverlord.executeUpdate();
                                assert c <= 1;
                                if (1 != c) {
                                    connection.rollback();
                                    // FIXME log
                                    throw ApiException.internalError();
                                }
                            } finally {
                                updateOverlord.close();
                            }

                            // find the authority
                            PreparedStatement selectAuthority = connection.prepareStatement("SELECT Overlord.public_host, Overlord.port FROM Overlord" +
                                    " WHERE access_key = ? AND local_key = ?");
                            try {
                                selectAuthority.setString(1, accessKey.toString());
                                selectAuthority.setString(2, localKey.toString());

                                ResultSet rs = selectAuthority.executeQuery();
                                try {
                                    assert "public_host".equals(rs.getMetaData().getColumnName(1));
                                    assert "port".equals(rs.getMetaData().getColumnName(2));
                                    if (!rs.next()) {
                                        // FIXME log this
                                        throw ApiException().internalError();
                                    } else {


                                        Authority authority = new Authority(rs.getString(1), rs.getInt(2));

                                        Overlord overlord = new Overlord();
                                        overlord.authority = authority;
                                        overlord.localKey = localKey;
                                        return overlord;
                                    }
                                } finally {
                                    rs.close();
                                }
                            } finally {
                                selectAuthority.close();
                            }

                        } finally {
                            // implied commit here
                            connection.setAutoCommit(true);
                        }
                    } catch (SQLException e) {
                        connection.rollback();
                        // try again
                    }
                }

                throw ApiException.internalError();
            } catch (SQLException e) {
                // FIXME log
                throw new ApiException(e);
            }
        }).take(1);
    }

    public Observable<ApiResult> justReleaseOverlordAuthority(final Authority authority) {
        return connectionSource.map((Connection connection) -> {
            // note: this does not delete from the status since it's easier to manage that synchronously with the status checks



            PreparedStatement selectLocalKey = connection.prepareStatement("UPDATE Overlord" +
                    " SET access_key = NULL, local_key = NULL" +
                    " WHERE public_host = ? AND Overlord.port = ?");

            selectLocalKey.setString(1, authority.host);
            selectLocalKey.setInt(2, authority.port);


            int c = selectLocalKey.executeUpdate();
            assert c <= 1;
            // succeed whether the authority was reserved (1 == c) or not (0 == c)
            return ApiResult.success();
        }).take(1);
    }

    // FIXME overlord status
    // FIXME overlord status is used to maintain reservations


    public Observable<Collection<Overlord>> justOverlords(NxId accessKey) {

        // FIXME read full overlord (including status)
    }



    // omitted permissions are not changed
    public Observable<ApiResult> justGrant(NxId accessKey, NxId grantKey, Permission.Mask ... permissionMasks) {

    }







    public Observable<ApiResult> justDirtyPermissions(NxId accessKey) {
        // FIXME dirty shared cache
    }

    public Observable<ApiResult> justDirtyOverlords(NxId accessKey) {
        //
        // FIXME dirty shared cache

        // FIXME update CloudFront
    }


    /////// MYSQL ///////

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception e) {
            // FIXME log
            throw new IllegalStateException(e);
        }
    }
}
