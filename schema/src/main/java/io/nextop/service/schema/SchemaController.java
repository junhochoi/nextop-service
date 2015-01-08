package io.nextop.service.schema;

import io.nextop.ApiStatus;
import io.nextop.ApiComponent;
import io.nextop.db.DataSourceProvider;
import org.apache.http.HttpStatus;
import org.flywaydb.core.Flyway;
import rx.Observable;

public final class SchemaController extends ApiComponent.Base {
    DataSourceProvider dataSourceProvider;

    public SchemaController(DataSourceProvider dataSourceProvider) {
        this.dataSourceProvider = dataSourceProvider;

        init = dataSourceProvider.init();
    }


    public Observable<ApiStatus> justUpgrade(String schemaName) {
        return dataSourceProvider.withDataSource().map(dataSource -> {
            Flyway flyway = new Flyway();
            flyway.setDataSource(dataSource);
            flyway.setLocations(String.format("classpath:/%s/db/migration", schemaName));
            flyway.migrate();

            return new ApiStatus(HttpStatus.SC_OK,
                    String.format("Schema %s upgraded to %s", schemaName, flyway.getTarget()));
        });
    }
}
