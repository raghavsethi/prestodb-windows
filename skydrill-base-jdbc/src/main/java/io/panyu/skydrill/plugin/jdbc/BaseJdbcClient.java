package io.panyu.skydrill.plugin.jdbc;

import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.panyu.skydrill.plugin.jdbc.metastore.JdbcMetastore;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static io.panyu.skydrill.plugin.jdbc.JdbcMetadata.containsHint;
import static io.panyu.skydrill.plugin.jdbc.JdbcSessionProperties.isViewPushdownEnabled;
import static io.panyu.skydrill.plugin.jdbc.JdbcMetadata.getCanonicalTableName;

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
    return getTableNames(null, schema);
  }

  @Override
  public List<SchemaTableName> getTableNames(ConnectorSession session, @Nullable String schema) {
    if ((session != null) && isViewPushdownEnabled(session)) {
      List<SchemaTableName> names = new ArrayList<>();
      names.addAll(listViews(null, schema));
      names.addAll(super.getTableNames(schema));
      return ImmutableList.copyOf(names);
    }
    return super.getTableNames(schema);
  }

  @Nullable
  @Override
  public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName) {
    return getTableHandle(null, schemaTableName);
  }

  @Nullable
  @Override
  public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName) {
    if (((session != null) && isViewPushdownEnabled(session)) || (containsHint(schemaTableName))) {
      SchemaTableName tableName = getCanonicalTableName(schemaTableName);
      Optional<String> viewData = metastore.getViewData(tableName);
      if (viewData.isPresent()) {
        return new JdbcTableHandle(
                connectorId,
                tableName,
                connectorId,
                tableName.getSchemaName(),
                tableName.getTableName());
      }
    }
    return super.getTableHandle(schemaTableName);
  }

  @Override
  public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle) {
    if (isViewPushdownEnabled(session) || isView(tableHandle)) {
      Optional<ViewDefinition> viewDefinition = metastore.getViewDefinition(tableHandle.getSchemaTableName());
      if (viewDefinition.isPresent()) {
        return viewDefinition.get().getColumns().stream()
                .map(y -> makeJdbcColumnHandle(connectorId, y))
                .collect(ImmutableList.toImmutableList());
      }
    }
    return super.getColumns(session, tableHandle);
  }

  @Override
  public PreparedStatement buildSql(Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
          throws SQLException {
    return new QueryBuilder(identifierQuote).buildSql(
            this,
            connection,
            split.getCatalogName(),
            split.getSchemaName(),
            split.getTableName(),
            columnHandles,
            split.getTupleDomain());
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
    if (isViewPushdownEnabled(session) || containsHint(prefix)) {
      return ImmutableMap.of();
    }

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
        metastore.getViewData(schemaTableName).ifPresent(s ->
          views.put(schemaTableName, new ConnectorViewDefinition(schemaTableName, Optional.empty(), s))
        );
      }
    }
    return views.build();
  }

  @Override
  public Optional<ViewDefinition> getViewDefinition(SchemaTableName viewName)  {
    return metastore.getViewDefinition(viewName);
  }

  protected JdbcColumnHandle makeJdbcColumnHandle(String connectorId, ViewDefinition.ViewColumn column) {
    switch (column.getType().getDisplayName()) {
      case "boolean":
        return new JdbcColumnHandle(connectorId, column.getName(), new JdbcTypeHandle(Types.BOOLEAN, 1, 0), BOOLEAN);
      case "bigint":
        return new JdbcColumnHandle(connectorId, column.getName(), new JdbcTypeHandle(Types.BIGINT, 8, 0), BIGINT);
      case "int":
      case "integer":
        return new JdbcColumnHandle(connectorId, column.getName(), new JdbcTypeHandle(Types.INTEGER, 4, 0), INTEGER);
      case "smallint":
        return new JdbcColumnHandle(connectorId, column.getName(), new JdbcTypeHandle(Types.SMALLINT, 2, 0), SMALLINT);
      case "tinyint":
        return new JdbcColumnHandle(connectorId, column.getName(), new JdbcTypeHandle(Types.TINYINT, 1, 0), TINYINT);
      case "decimal":
      case "double":
        return new JdbcColumnHandle(connectorId, column.getName(), new JdbcTypeHandle(Types.DOUBLE, 8, 0), DOUBLE);
      case "real":
        return new JdbcColumnHandle(connectorId, column.getName(), new JdbcTypeHandle(Types.REAL, 8, 0), REAL);
      case "timestamp":
        return new JdbcColumnHandle(connectorId, column.getName(), new JdbcTypeHandle(Types.TIMESTAMP, 8, 0), TIMESTAMP);
      case "date":
        return new JdbcColumnHandle(connectorId, column.getName(), new JdbcTypeHandle(Types.DATE, 8, 0), DATE);
      case "time":
        return new JdbcColumnHandle(connectorId, column.getName(), new JdbcTypeHandle(Types.TIME, 4, 0), TIME);
      case "varbinary":
        return new JdbcColumnHandle(connectorId, column.getName(), new JdbcTypeHandle(Types.VARBINARY, 8, 0), VARBINARY);
      case "char":
        return new JdbcColumnHandle(connectorId, column.getName(), new JdbcTypeHandle(Types.CHAR, 1, 0), column.getType());
    }

    if (column.getType() instanceof VarcharType) {
      return new JdbcColumnHandle(connectorId, column.getName(), new JdbcTypeHandle(Types.VARCHAR,
              ((VarcharType) column.getType()).getLengthSafe(), 0), column.getType());
    }

    throw new RuntimeException("unsupported column type " + column.toString());
  }

  private boolean isView(JdbcTableHandle tableHandle) {
      return metastore.isView(tableHandle.getSchemaTableName());
  }
}
