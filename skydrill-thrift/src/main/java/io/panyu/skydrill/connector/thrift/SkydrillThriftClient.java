package io.panyu.skydrill.connector.thrift;

import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import io.airlift.drift.client.DriftClient;
import io.airlift.log.Logger;
import io.panyu.skydrill.plugin.jdbc.metastore.JdbcMetastore;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SkydrillThriftClient
        implements DriftClient<PrestoThriftService>
{
    private static final Logger log = Logger.get(SkydrillThriftClient.class);

    private final JdbcConnectorId connectorId;
    private final DriftClient<PrestoThriftService> client;
    private final JdbcMetastore metastore;

    @Inject
    public SkydrillThriftClient(DriftClient<PrestoThriftService> client,
                                JdbcConnectorId connectorId,
                                //SkydrillThriftConfig config,
                                JdbcMetastore metastore) {
        this.connectorId = requireNonNull(connectorId, "ConnectorId is null");
        this.client = requireNonNull(client, "DriftClient is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    public PrestoThriftService get() {
        return client.get();
    }

    @Override
    public PrestoThriftService get(Optional<String> addressSelectionContext) {
        return client.get(addressSelectionContext);
    }

    @Override
    public PrestoThriftService get(Map<String, String> headers) {
        return client.get(headers);
    }

    @Override
    public PrestoThriftService get(Optional<String> addressSelectionContext, Map<String, String> headers) {
        return client.get(addressSelectionContext, headers);
    }

    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace) {
        metastore.createView(viewName, viewData, replace);
    }

    public void dropView(ConnectorSession session, SchemaTableName viewName) {
        metastore.dropView(viewName);
    }

    public List<SchemaTableName> listViews(ConnectorSession session, String schema) {
        return metastore.listViews(schema);
    }

    public Optional<String> getViewData(SchemaTableName viewName) {
        return metastore.getViewData(viewName);
    }

    protected Optional<ViewDefinition> getViewDefinition(SchemaTableName viewName) {
        return metastore.getViewDefinition(viewName);
    }

}
