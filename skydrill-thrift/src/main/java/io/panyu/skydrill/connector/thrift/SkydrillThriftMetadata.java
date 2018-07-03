package io.panyu.skydrill.connector.thrift;

import com.facebook.presto.connector.thrift.ThriftHeaderProvider;
import com.facebook.presto.connector.thrift.annotations.ForMetadataRefresh;
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

public class SkydrillThriftMetadata
        extends com.facebook.presto.connector.thrift.ThriftMetadata
{
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
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace) {
        client.createView(session, viewName, viewData, replace);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName) {
        client.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schema) {
        return client.listViews(session, schema);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix) {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        List<SchemaTableName> tableNames;
        if (prefix.getTableName() != null) {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tableNames = listViews(session, prefix.getSchemaName());
        }

        for (SchemaTableName schemaTableName : tableNames) {
            ConnectorTableHandle tableHandle = getTableHandle(null, schemaTableName);
            if (tableHandle == null) {
                views.put(schemaTableName, new ConnectorViewDefinition(
                        schemaTableName,
                        Optional.empty(),
                        client.getViewData(schemaTableName)));
            }
        }

        return views.build();
    }
}
