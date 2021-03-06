package io.panyu.skydrill.connector.thrift;

import com.facebook.presto.connector.thrift.ThriftColumnHandle;
import com.facebook.presto.connector.thrift.ThriftHeaderProvider;
import com.facebook.presto.connector.thrift.ThriftMetadata;
import com.facebook.presto.connector.thrift.ThriftTableHandle;
import com.facebook.presto.connector.thrift.annotations.ForMetadataRefresh;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.panyu.skydrill.plugin.jdbc.JdbcSessionProperties.isViewPushdownEnabled;

public class SkydrillThriftMetadata
        extends ThriftMetadata
{
    private static final String PD_HINT_TOKEN = "_";
    private final SkydrillThriftClient client;

    @Inject
    public SkydrillThriftMetadata(
            SkydrillThriftClient client,
            ThriftHeaderProvider thriftHeaderProvider,
            TypeManager typeManager,
            @ForMetadataRefresh Executor metadataRefreshExecutor) {
        super(client, thriftHeaderProvider, typeManager, metadataRefreshExecutor);
        this.client = client;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schema)
    {
        if (isViewPushdownEnabled(session)) {
            ImmutableList.Builder<SchemaTableName> builder = new ImmutableList.Builder<>();
            return builder.addAll(listViews(session, Optional.of(schema)))
                    .addAll(super.listTables(session, schema))
                    .build();
        }
        return super.listTables(session, schema);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (isViewPushdownEnabled(session) || containsHint(tableName)) {
            Optional<String> viewData = client.getViewData(getCanonicalTableName(tableName));
            if (viewData.isPresent()) {
                return new ThriftTableHandle(getCanonicalTableName(tableName));
            }
        }
        return super.getTableHandle(session, tableName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ThriftTableHandle handle = ((ThriftTableHandle) tableHandle);
        if (isViewPushdownEnabled(session) || containsHint(handle)) {
            Optional<ViewDefinition> viewDefinition =
                    client.getViewDefinition(getCanonicalTableName(handle));
            if (viewDefinition.isPresent()) {
                return viewDefinition.get().getColumns().stream()
                        .collect(toImmutableMap(ViewDefinition.ViewColumn::getName,
                                x -> new ThriftColumnHandle(x.getName(), x.getType(), x.toString(), false)));
            }
        }
        return super.getColumnHandles(session, tableHandle);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        client.createView(session, viewName, viewData, replace);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        client.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schema)
    {
        return schema.map(s -> client.listViews(session, s)).orElseGet(ImmutableList::of);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (isViewPushdownEnabled(session) || containsHint(prefix.getTableName())) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        List<SchemaTableName> tableNames;
        if (prefix.getTableName() != null) {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tableNames = listViews(session, Optional.of(prefix.getSchemaName()));
        }

        for (SchemaTableName schemaTableName : tableNames) {
            client.getViewData(schemaTableName).ifPresent(x ->
                views.put(schemaTableName, new ConnectorViewDefinition(schemaTableName, Optional.empty(), x))
            );
        }

        return views.build();
    }

    private boolean containsHint(ThriftTableHandle tableHandle) {
        return containsHint(tableHandle.getTableName());
    }

    private boolean containsHint(SchemaTableName schemaTableName) {
        return containsHint(schemaTableName.getTableName());
    }

    private boolean containsHint(String tableName) {
        return tableName.endsWith(PD_HINT_TOKEN);
    }

    private SchemaTableName getCanonicalTableName(ThriftTableHandle tableHandle) {
        return new SchemaTableName(tableHandle.getSchemaName(), getCanonicalTableName(tableHandle.getTableName()));
    }

    private SchemaTableName getCanonicalTableName(SchemaTableName schemaTableName) {
        if (!containsHint(schemaTableName))
            return schemaTableName;
        return new SchemaTableName(schemaTableName.getSchemaName(), getCanonicalTableName(schemaTableName.getTableName()));
    }

    private String getCanonicalTableName(String tableName) {
        return containsHint(tableName)? tableName.substring(0, tableName.length() - 1): tableName;
    }

}
