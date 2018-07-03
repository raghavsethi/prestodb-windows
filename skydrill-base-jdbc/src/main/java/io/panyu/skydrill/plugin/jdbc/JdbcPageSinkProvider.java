package io.panyu.skydrill.plugin.jdbc;

import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

public class JdbcPageSinkProvider
        extends com.facebook.presto.plugin.jdbc.JdbcPageSinkProvider
{
  private final JdbcClient jdbcClient;

  @Inject
  public JdbcPageSinkProvider(JdbcClient jdbcClient) {
    super(jdbcClient);
    this.jdbcClient = jdbcClient;
  }

  @Override
  public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle) {
    return new JdbcPageSink((JdbcOutputTableHandle) tableHandle, jdbcClient);
  }

  @Override
  public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle) {
    return new JdbcPageSink((JdbcOutputTableHandle) tableHandle, jdbcClient);
  }
}
