package io.nextop.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public final class DbUtils {
    private DbUtils() { }

    public static final class Asserts {
        private Asserts() { }

        public static boolean columnNames(ResultSet rs, String ... columnNames) throws SQLException {
            ResultSetMetaData m = rs.getMetaData();

            int n = columnNames.length;
            assert m.getColumnCount() == n;
            for (int i = 0; i < n; ++i) {
                assert m.getColumnName(i + 1).equals(columnNames[i]);
            }

            return true;
        }
    }


}
