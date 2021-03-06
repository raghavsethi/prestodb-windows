package io.panyu.skydrill.plugin.jdbc.metastore;

import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface JdbcMetastore extends Cloneable {
  void createView(SchemaTableName viewName, String viewData, boolean replace);

  void dropView(SchemaTableName viewName);

  List<SchemaTableName> listViews(String schema);

  Map<SchemaTableName, ConnectorViewDefinition> getViews(SchemaTablePrefix prefix);

  Optional<String> getViewData(SchemaTableName viewName);

  Optional<ViewDefinition> getViewDefinition(SchemaTableName viewName);

  boolean isView(SchemaTableName schemaTableName);
}
