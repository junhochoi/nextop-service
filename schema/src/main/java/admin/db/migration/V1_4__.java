package admin.db.migration;

import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import java.sql.Connection;

public class V1_4__ implements JdbcMigration {
    @Override
    public void migrate(Connection connection) throws Exception {
        // Do nothing - this is a test!
    }
}
