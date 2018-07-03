package io.panyu.skydrill.plugin.jdbc.metastore;

import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;

import java.util.List;
import java.util.Map;

public interface JdbcMetastore {
  void createView(SchemaTableName viewName, String viewData, boolean replace);

  void dropView(SchemaTableName viewName);

  List<SchemaTableName> listViews(String schema);

  Map<SchemaTableName, ConnectorViewDefinition> getViews(SchemaTablePrefix prefix);

  String getViewData(SchemaTableName viewName);

  ViewDefinition getViewDefinition(SchemaTableName viewName);
}
