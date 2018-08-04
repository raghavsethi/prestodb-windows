package io.panyu.skydrill.plugin.jdbc;

import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class JdbcMetadata
        extends com.facebook.presto.plugin.jdbc.JdbcMetadata {
  private final JdbcClient jdbcClient;

  public JdbcMetadata(JdbcClient jdbcClient, boolean allowDropTable) {
    super(jdbcClient, allowDropTable);
    this.jdbcClient = jdbcClient;
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
    return jdbcClient.getTableNames(session, schemaNameOrNull);
  }

  @Override
  public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
    ConnectorTableHandle tableHandle = getCanonicalTableHandle((JdbcTableHandle) table);
    return super.getTableLayouts(session, tableHandle, constraint, desiredColumns);
  }

  @Override
  public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
    ConnectorTableLayoutHandle layoutHandle = getCanonicalTableLayoutHandle((JdbcTableLayoutHandle) handle);
    return super.getTableLayout(session, layoutHandle);
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
    ConnectorTableHandle table = getCanonicalTableHandle((JdbcTableHandle) tableHandle);
    return super.getColumnHandles(session, table);
  }

  @Override
  public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
    return super.listTableColumns(session, getCanonicalTablePrefix(prefix));
  }

  @Override
  public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
    ConnectorTableHandle table = getCanonicalTableHandle((JdbcTableHandle) tableHandle);
    return super.getColumnMetadata(session, table, columnHandle);
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
  public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schema) {
    return schema.map(s -> jdbcClient.listViews(session, s)).orElseGet(ImmutableList::of);
  }

  @Override
  public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix) {
    return jdbcClient.getViews(session, prefix);
  }

  public static boolean containsHint(SchemaTableName schemaTableName) {
    return schemaTableName.getTableName().endsWith(BaseJdbcConfig.PD_SUFFIX);
  }

  protected static boolean containsHint(SchemaTablePrefix schemaTablePrefix) {
    return schemaTablePrefix.getTableName().endsWith(BaseJdbcConfig.PD_SUFFIX);
  }

  protected static boolean containsHint(JdbcTableHandle tableHandle) {
    return tableHandle.getTableName().endsWith(BaseJdbcConfig.PD_SUFFIX);
  }

  private static boolean containsHint(JdbcTableLayoutHandle tableLayoutHandle) {
    return containsHint(tableLayoutHandle.getTable());
  }

  public static SchemaTableName getCanonicalTableName(SchemaTableName schemaTableName) {
    if (!containsHint(schemaTableName))
      return schemaTableName;
    return new SchemaTableName(schemaTableName.getSchemaName(), getCanonicalTableName(schemaTableName.getTableName()));
  }

  private static SchemaTablePrefix getCanonicalTablePrefix(SchemaTablePrefix schemaTablePrefix) {
    if (!containsHint(schemaTablePrefix))
      return schemaTablePrefix;
    return new SchemaTablePrefix(schemaTablePrefix.getSchemaName(), getCanonicalTableName(schemaTablePrefix.getTableName()));
  }

  protected static String getCanonicalTableName(String tableName) {
    return tableName.endsWith(BaseJdbcConfig.PD_SUFFIX)? tableName.substring(0, tableName.length() - 1): tableName;
  }

  protected static JdbcTableHandle getCanonicalTableHandle(JdbcTableHandle tableHandle) {
    if (!containsHint(tableHandle))
      return tableHandle;
    return new JdbcTableHandle(tableHandle.getConnectorId(),
            getCanonicalTableName(tableHandle.getSchemaTableName()),
            tableHandle.getCatalogName(),
            tableHandle.getSchemaName(),
            getCanonicalTableName(tableHandle.getTableName()));
  }

  private static JdbcTableLayoutHandle getCanonicalTableLayoutHandle(JdbcTableLayoutHandle layoutHandle) {
    if (!containsHint(layoutHandle))
      return layoutHandle;
    return new JdbcTableLayoutHandle(getCanonicalTableHandle(layoutHandle.getTable()), layoutHandle.getTupleDomain());
  }
}
