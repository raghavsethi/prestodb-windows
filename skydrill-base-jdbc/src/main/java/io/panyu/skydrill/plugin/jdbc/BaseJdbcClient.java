package io.panyu.skydrill.plugin.jdbc;

import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.panyu.skydrill.plugin.jdbc.metastore.JdbcMetastore;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BaseJdbcClient
        extends com.facebook.presto.plugin.jdbc.BaseJdbcClient
        implements JdbcClient
{
  private final BaseJdbcConfig config;
  private final JdbcMetastore metastore;

  public BaseJdbcClient(JdbcConnectorId connectorId,
                        BaseJdbcConfig config,
                        String identifierQuote,
                        ConnectionFactory connectionFactory,
                        JdbcMetastore metastore) {
    super(connectorId, config, identifierQuote, connectionFactory);
    this.config = config;
    this.metastore = metastore;
  }

  @Override
  public List<SchemaTableName> getTableNames(@Nullable String schema) {
    List<SchemaTableName> names = new ArrayList<>();
    names.addAll(listViews(null, schema));
    names.addAll(super.getTableNames(schema));
    return ImmutableList.copyOf(names);
  }

  @Override
  public PreparedStatement buildSql(Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
          throws SQLException {
    String schemaName = split.getSchemaName();
    String tableName = split.getTableName();

    return new QueryBuilder(identifierQuote).buildSql(
            this,
            connection,
            split.getCatalogName(),
            schemaName,
            tableName,
            columnHandles,
            split.getTupleDomain(),
            metastore);
  }

  @Override
  public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace) {
    metastore.createView(viewName, viewData, replace);
  }

  @Override
  public void dropView(ConnectorSession session, SchemaTableName viewName) {
    metastore.dropView(viewName);
  }

  @Override
  public List<SchemaTableName> listViews(ConnectorSession session, String schema) {
    return metastore.listViews(schema);
  }

  @Override
  public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix) {
    ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
    List<SchemaTableName> tableNames;
    if (prefix.getTableName() != null) {
      tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    } else {
      tableNames = listViews(session, prefix.getSchemaName());
    }

    for (SchemaTableName schemaTableName : tableNames) {
      JdbcTableHandle tableHandle = getTableHandle(schemaTableName);
      if (tableHandle == null) {
        views.put(schemaTableName, new ConnectorViewDefinition(
                schemaTableName,
                Optional.empty(),
                metastore.getViewData(schemaTableName)));
      }
    }
    return views.build();
  }

}
