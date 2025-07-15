package io.kestra.plugin.jdbc.snowflake;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.mockito.Mockito.*;

class QueryTest {

    private Query query;
    private Statement statementMock;
    private Connection connectionMock;

    @BeforeEach
    void setUp() {
        this.query = new Query();
        this.statementMock = mock(Statement.class);
        this.connectionMock = mock(Connection.class);

        this.query.setRunningStatement(statementMock);
        this.query.setRunningConnection(connectionMock);
    }

    @Test
    void shouldCancelStatementAndCloseConnectionOnKill() throws SQLException {
        // given
        when(statementMock.isClosed()).thenReturn(false);
        when(connectionMock.isClosed()).thenReturn(false);

        // when
        query.kill();

        // then
        verify(statementMock, times(1)).cancel();
        verify(connectionMock, times(1)).close();
    }

    @Test
    void shouldNotCallCancelIfStatementIsNull() throws SQLException {
        this.query.setRunningStatement(null);

        when(connectionMock.isClosed()).thenReturn(false);
        query.kill();

        verify(connectionMock, times(1)).close();
        // no interaction with statement
        verifyNoInteractions(statementMock);
    }

    @Test
    void shouldNotCallCloseIfConnectionIsNull() throws SQLException {
        this.query.setRunningConnection(null);

        when(statementMock.isClosed()).thenReturn(false);
        query.kill();

        verify(statementMock, times(1)).cancel();
        // no interaction with connection
        verifyNoInteractions(connectionMock);
    }

    @Test
    void shouldIgnoreClosedResources() throws SQLException {
        when(statementMock.isClosed()).thenReturn(true);
        when(connectionMock.isClosed()).thenReturn(true);

        query.kill();

        verify(statementMock, never()).cancel();
        verify(connectionMock, never()).close();
    }

    @Test
    void shouldIgnoreExceptionThrownDuringCancelAndClose() throws SQLException {
        when(statementMock.isClosed()).thenReturn(false);
        when(connectionMock.isClosed()).thenReturn(false);

        doThrow(new SQLException("Cancel error")).when(statementMock).cancel();
        doThrow(new SQLException("Close error")).when(connectionMock).close();

        // should not crash
        query.kill();

        verify(statementMock).cancel();
        verify(connectionMock).close();
    }
}
