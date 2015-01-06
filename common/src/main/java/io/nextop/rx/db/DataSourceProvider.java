package io.nextop.rx.db;

import com.google.gson.JsonObject;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.nextop.ApiComponent;
import io.nextop.rx.util.FixedPool;
import org.apache.http.client.utils.URIBuilder;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.Subscription;
import rx.subjects.BehaviorSubject;
import rx.subjects.Subject;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

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


        init = ApiComponent.layerInit(Arrays.asList(),
                () -> {
                    // FIXME take(1)
                    managerSubscription = configSource.subscribe(dataSourceManager());
                },
                () -> {
                    managerSubscription.unsubscribe();
                });
    }


    private Observer<Config> dataSourceManager() {
        return new Observer<Config>() {
            @Nullable
            FixedPool<Connection> connectionPool = null;

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
                close();
            }
            @Override
            public void onError(Throwable e) {
                close();
            }


            private void close() {
                if (null != connectionPool) {
                    // important: this blocks until all subscriptions are fulfilled
                    connectionPool.close();
                    connectionPool = null;
                }
            }
        };
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


    public Observable<Connection> justConnection() {
        return singleConnectionSourceSubject.flatMap(singleSource -> singleSource);
    }

    public Observable<DataSource> justDataSource() {
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
