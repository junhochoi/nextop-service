package io.nextop.db;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import io.nextop.ApiComponent;
import io.nextop.rx.util.FixedPool;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.Subscription;
import rx.subjects.BehaviorSubject;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class DataSourceProvider extends ApiComponent.Base {
    public static final class Config {
        public String scheme;
        public String host;
        public int port;
        public String db;
        public String user;
        public String password;

        public int maxSize = 1;
    }


    Scheduler scheduler;

    BehaviorSubject<DataSource> dataSourceSubject;
    BehaviorSubject<Observable<Connection>> singleConnectionSourceSubject;

    // INTERNAL SUBSCRIPTIONS
    @Nullable
    private Subscription managerSubscription = null;



    public DataSourceProvider(Scheduler scheduler, Observable<Config> configSource) {
        this.scheduler = scheduler;

        dataSourceSubject = BehaviorSubject.create();
        singleConnectionSourceSubject = BehaviorSubject.create();

        DataSourceManager manager = new DataSourceManager();
        init = ApiComponent.init("Data Source Provider",
                statusSink -> {
                    managerSubscription = configSource.take(1).subscribe(manager);
                },
                () -> {
                    managerSubscription.unsubscribe();
                    manager.close();
                });
    }



    private class DataSourceManager implements Observer<Config> {
        @Nullable
        private FixedPool<Connection> connectionPool = null;

        @Override
        public void onNext(Config config) {
            close();
            assert null == connectionPool;

            DataSource dataSource = createDataSource(config);

            connectionPool = new FixedPool<Connection>(config.maxSize,
                    () -> {
                        try {
                            return dataSource.getConnection();
                        } catch (SQLException e) {
                            throw new IllegalStateException(e);
                        }
                    },
                    connection -> {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            throw new IllegalStateException(e);
                        }
                    },
                    connection -> {
                        try {
                            return !connection.isClosed();
                        } catch (SQLException e) {
                            throw new IllegalStateException(e);
                        }
                    });

            dataSourceSubject.onNext(dataSource);
            singleConnectionSourceSubject.onNext(connectionPool.getSingleObservable());
        }
        @Override
        public void onCompleted() {
            // ignore
        }
        @Override
        public void onError(Throwable e) {
            // ignore
        }


        void close() {
            if (null != connectionPool) {
                connectionPool.close();
                connectionPool = null;
            }
        }
    }

    private DataSource createDataSource(Config config) {
        switch (config.scheme) {
            case "mysql": {
                MysqlDataSource mysqlDataSource = new MysqlDataSource();
                mysqlDataSource.setServerName(config.host);
                mysqlDataSource.setPort(config.port);
                mysqlDataSource.setDatabaseName(config.db);
                mysqlDataSource.setUser(config.user);
                mysqlDataSource.setPassword(config.password);
                return mysqlDataSource;
            }
            default:
                throw new IllegalArgumentException(config.scheme);
        }
    }


    public Observable<Connection> withConnection() {
        return singleConnectionSourceSubject.flatMap(singleSource -> singleSource);
    }

    public Observable<DataSource> withDataSource() {
        return dataSourceSubject.take(1);
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
