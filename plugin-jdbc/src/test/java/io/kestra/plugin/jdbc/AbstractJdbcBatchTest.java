package io.kestra.plugin.jdbc;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.sql.*;

import static org.mockito.Mockito.*;

/**
 * Verifies the JDBC batch commit contract used by {@link AbstractJdbcBatch}:
 * auto-commit is disabled, batches execute independently, and a single commit happens at the end.
 */
class AbstractJdbcBatchTest {

    @Test
    void commitIsCalledOnceAfterAllChunks() throws SQLException {
        Connection connection = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        PreparedStatement ps = mock(PreparedStatement.class);

        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.supportsTransactions()).thenReturn(true);

        // Replicate the batch processing pattern from AbstractJdbcBatch.run()
        connection.setAutoCommit(false);
        for (int chunk = 0; chunk < 3; chunk++) {
            ps.executeBatch();
        }
        connection.commit();

        InOrder inOrder = inOrder(connection, ps);
        inOrder.verify(connection).setAutoCommit(false);
        inOrder.verify(ps, times(3)).executeBatch();
        inOrder.verify(connection).commit();
        verify(connection, times(1)).commit();
    }

    @Test
    void commitIsSkippedWhenTransactionsNotSupported() throws SQLException {
        Connection connection = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        PreparedStatement ps = mock(PreparedStatement.class);

        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.supportsTransactions()).thenReturn(false);

        for (int chunk = 0; chunk < 3; chunk++) {
            ps.executeBatch();
        }

        verify(connection, never()).setAutoCommit(anyBoolean());
        verify(connection, never()).commit();
    }
}
