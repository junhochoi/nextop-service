package io.nextop.service.admin;

import io.nextop.ApiComponent;
import io.nextop.ApiException;
import io.nextop.ApiStatus;
import io.nextop.service.*;
import io.nextop.service.m.Overlord;
import io.nextop.service.m.OverlordStatus;
import io.nextop.util.DbUtils;
import rx.Observable;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/** Model for the service admin (dns, hyperlord) */
// TODO verify that Observables return SafeSubscriber subscriptions
public class AdminModel extends ApiComponent.Base {
    private final AdminContext context;


    public AdminModel(AdminContext context) {
        this.context = context;

    }


    /////// PERMISSIONS ///////

    public <T> Observable<T> requirePermissions(Observable<T> source, NxId accessKey, Collection<NxId> grantKeys, Permission.Mask... permissionMasks) {
        return context.dataSourceProvider.withConnection().map((Connection connection) -> {
            context.log.message("adminModel.requirePermissions");

            try {
                // from all given grant keys, gather all permission names with value=true
                // then test if the given masks apply
                final PreparedStatement selectPermissionNames = connection.prepareStatement("SELECT GrantKeyPermission.permission_name" +
                        " FROM GrantKey INNER JOIN GrantKeyPermission ON GrantKey.grant_key = GrantKeyPermission.grant_key" +
                        " WHERE GrantKey.access_key = ? AND GrantKey.grant_key = ? AND GrantKeyPermission.permission_value = true");
                try {
                    Set<Permission> grantKeysPermissions = grantKeys.stream().flatMap(grantKey -> {
                        List<Permission> grantKeyPermissions = new ArrayList<Permission>(4);
                        try {
                            selectPermissionNames.setString(1, accessKey.toString());
                            selectPermissionNames.setString(2, grantKey.toString());
                            ResultSet rs = selectPermissionNames.executeQuery();
                            try {
                                assert DbUtils.Asserts.columnNames(rs,
                                        "permission_name");
                                while (rs.next()) {
                                    grantKeyPermissions.add(Permission.valueOf(rs.getString(1)));
                                }
                            } finally {
                                rs.close();
                            }
                        } catch (SQLException e) {
                            throw new ApiException(e);
                        }
                        return grantKeyPermissions.stream();
                    }).collect(Collectors.toSet());

                    boolean pass = true;
                    for (Permission.Mask permissionMask : permissionMasks) {
                        if (permissionMask.mask != grantKeysPermissions.contains(permissionMask.permission)) {
                            pass = false;
                            break;
                        }
                    }
                    context.log.message("adminModel.requirePermissions", "pass = %s", pass);
                    return pass;
                } finally {
                    selectPermissionNames.close();
                }
            } catch (SQLException e) {
                throw new ApiException(e);
            }
        }).flatMap((Boolean authorized) -> {
            if (authorized) {
                return source;
            } else {
                return Observable.error(new ApiException(ApiStatus.unauthorized()));
            }
        });
    }


    /////// API ///////

    /** just* API methods emit one value then call onComplete */

    // FIXME justCreateAccessKey
    // FIXME justMarkAccessKeyTerminating
    // FIXME justRemoveAccessKey
    // FIXME justMarkOverlordTerminating(localKey)
    //

    public Observable<Overlord> justCreateOverlord(NxId accessKey) {
        return context.dataSourceProvider.withConnection().map((Connection connection) -> {
            NxId localKey = NxId.create();

            try {
                // retry is needed when the free slot from the transaction is taken before the transaction commits
                for (; ; ) {
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


                            Authority authority;
                            // find the authority
                            PreparedStatement selectAuthority = connection.prepareStatement("SELECT Overlord.public_host, Overlord.port FROM Overlord" +
                                    " WHERE access_key = ? AND local_key = ?");
                            try {
                                selectAuthority.setString(1, accessKey.toString());
                                selectAuthority.setString(2, localKey.toString());

                                ResultSet rs = selectAuthority.executeQuery();
                                try {
                                    assert DbUtils.Asserts.columnNames(rs,
                                            "public_host",
                                            "port");
                                    if (!rs.next()) {
                                        // FIXME log this
                                        throw ApiException.internalError();
                                    } else {
                                        authority = new Authority(rs.getString(1), rs.getInt(2));
                                    }
                                } finally {
                                    rs.close();
                                }
                            } finally {
                                selectAuthority.close();
                            }


                            Overlord overlord = new Overlord();
                            overlord.authority = authority;
                            overlord.localKey = localKey;
                            return overlord;
                        } finally {
                            // implicitly commit
                            connection.setAutoCommit(true);
                        }
                    } catch (SQLException e) {
                        connection.rollback();
                        // try again
                    }
                }
            } catch (SQLException e) {
                // FIXME log
                throw new ApiException(e);
            }
        }).take(1);
    }

    public Observable<ApiStatus> justRemoveOverlord(Authority authority) {
        return context.dataSourceProvider.withConnection().map((Connection connection) -> {
            // note: this does not delete from the status since it's easier to manage that synchronously with the status checks
            try {
                PreparedStatement selectLocalKey = connection.prepareStatement("UPDATE Overlord" +
                        " SET access_key = NULL, local_key = NULL" +
                        " WHERE public_host = ? AND Overlord.port = ?");
                try {
                    selectLocalKey.setString(1, authority.host);
                    selectLocalKey.setInt(2, authority.port);

                    int c = selectLocalKey.executeUpdate();
                    assert c <= 1;
                    // continue whether the authority was reserved (1 == c) or not (0 == c)
                } finally {
                    selectLocalKey.close();
                }
            } catch (SQLException e) {
                throw new ApiException(e);
            }

            return ApiStatus.ok();
        }).take(1);
    }

    // FIXME overlord status
    // FIXME overlord status is used to maintain reservations

    public Observable<Collection<Overlord>> justOverlords(NxId accessKey) {
        return context.dataSourceProvider.withConnection().map((Connection connection) -> {
            Collection<Overlord> overlords = new ArrayList<Overlord>(16);
            try {
                PreparedStatement selectOverlord = connection.prepareStatement("SELECT Overlord.public_host, Overlord.port, Overlord.local_key," +
                        " OverlordStatus.package_tag, OverlordStatus.monitor_up, OverlordStatus.terminating FROM Overlord" +
                        " LEFT JOIN OverlordStatus ON Overlord.local_key = OverlordStatus.local_key" +
                        " WHERE Overlord.access_key = ?");
                try {
                    selectOverlord.setString(1, accessKey.toString());

                    ResultSet rs = selectOverlord.executeQuery();
                    try {
                        assert DbUtils.Asserts.columnNames(rs,
                                "public_host",
                                "port",
                                "local_key",
                                "package_tag",
                                "monitor_up",
                                "terminating");
                        while (rs.next()) {
                            Overlord overlord = new Overlord();
                            overlord.authority = new Authority(rs.getString(1), rs.getInt(2));
                            overlord.localKey = NxId.valueOf(rs.getString(3));

                            OverlordStatus status = new OverlordStatus();
                            status.packageTag = rs.getString(4);
                            status.monitorUp = rs.getBoolean(5);
                            status.terminating = rs.getBoolean(6);
                            overlord.status = status;

                            overlords.add(overlord);
                        }
                    } finally {
                        rs.close();
                    }
                } finally {
                    selectOverlord.close();
                }
            } catch (SQLException e) {
                throw new ApiException(e);
            }

            return overlords;
        }).take(1);
    }

    // omitted permissions are not changed
    public Observable<ApiStatus> justGrant(NxId accessKey, NxId grantKey, Permission.Mask ... permissionMasks) {
        return context.dataSourceProvider.withConnection().map((Connection connection) -> {
            try {
                connection.setAutoCommit(false);
                try {
                    PreparedStatement insertGrantKey = connection.prepareStatement("INSERT IGNORE INTO GrantKey" +
                            " (access_key, grant_key) VALUES (?, ?)");
                    try {
                        insertGrantKey.setString(1, accessKey.toString());
                        insertGrantKey.setString(2, grantKey.toString());

                        insertGrantKey.execute();
                        // continue in any case
                    } finally {
                        insertGrantKey.close();
                    }

                    PreparedStatement replaceGrantKeyPermission = connection.prepareStatement("REPLACE INTO GrantKeyPermission" +
                            " (grant_key, permission_name, permission_value) VALUES (?, ?, ?)");
                    try {
                        for (Permission.Mask permissionMask : permissionMasks) {
                            replaceGrantKeyPermission.setString(1, grantKey.toString());
                            replaceGrantKeyPermission.setString(2, permissionMask.permission.toString());
                            replaceGrantKeyPermission.setBoolean(3, permissionMask.mask);
                            replaceGrantKeyPermission.execute();
                            // continue in any case
                        }
                    } finally {
                        replaceGrantKeyPermission.close();
                    }
                } finally {
                    // implicitly commit
                    connection.setAutoCommit(true);
                }

                return ApiStatus.ok();
            } catch (SQLException e) {
                throw new ApiException(e);
            }
        });
    }


    public Observable<ApiStatus> justDirtyPermissions(NxId accessKey) {
        // TODO currently no caches
        // TODO dirty shared cache
        return Observable.just(ApiStatus.ok());
    }

    public Observable<ApiStatus> justDirtyOverlords(NxId accessKey) {
        // TODO currently no caches
        // TODO dirty shared cache
        // TODO update CloudFront
        return Observable.just(ApiStatus.ok());
    }

}
