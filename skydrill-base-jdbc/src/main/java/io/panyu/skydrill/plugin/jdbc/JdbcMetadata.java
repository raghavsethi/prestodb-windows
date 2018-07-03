package io.panyu.skydrill.plugin.jdbc;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;

import java.util.List;
import java.util.Map;

public class JdbcMetadata
        extends com.facebook.presto.plugin.jdbc.JdbcMetadata {
  private final JdbcClient jdbcClient;

  public JdbcMetadata(JdbcClient jdbcClient, boolean allowDropTable) {
    super(jdbcClient, allowDropTable);
    this.jdbcClient = jdbcClient;
  }

  @Override
  public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace) {
    jdbcClient.createView(session, viewName, viewData, replace);
  }

  @Override
  public void dropView(ConnectorSession session, SchemaTableName viewName) {
    jdbcClient.dropView(session, viewName);
  }

  @Override
  public List<SchemaTableName> listViews(ConnectorSession session, String schema) {
    return jdbcClient.listViews(session, schema);
  }

  @Override
  public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix) {
    return jdbcClient.getViews(session, prefix);
  }
  
}
