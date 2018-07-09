package io.panyu.skydrill.connector.thrift;

import com.facebook.presto.connector.thrift.ThriftColumnHandle;
import com.facebook.presto.connector.thrift.ThriftHeaderProvider;
import com.facebook.presto.connector.thrift.ThriftSplitManager;
import com.facebook.presto.connector.thrift.ThriftTableLayoutHandle;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.drift.client.DriftClient;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class SkydrillThriftSplitManager
        extends ThriftSplitManager {

    private final SkydrillThriftClient client;

    @Inject
    public SkydrillThriftSplitManager(DriftClient<PrestoThriftService> driftClient,
                                      ThriftHeaderProvider thriftHeaderProvider,
                                      SkydrillThriftClient client) {
        super(driftClient, thriftHeaderProvider);
        this.client = requireNonNull(client, "SkydrillThriftClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
                                          ConnectorSession session,
                                          ConnectorTableLayoutHandle layout,
                                          SplitSchedulingStrategy splitSchedulingStrategy) {
        //ConnectorTableLayoutHandle layoutHandle = getTableLayoutHandle(layout);
        return super.getSplits(transactionHandle, session, layout, splitSchedulingStrategy);
    }

    private ConnectorTableLayoutHandle getTableLayoutHandle(ConnectorTableLayoutHandle layoutHandle) {
        ThriftTableLayoutHandle layout = (ThriftTableLayoutHandle) layoutHandle;
        Map<ColumnHandle, Domain> domains = layout.getConstraint().getDomains().orElse(Collections.emptyMap());

        SchemaTableName table = new SchemaTableName(layout.getSchemaName(), layout.getTableName());
        String viewData =  client.getViewData(table);
        Type viewDataType = VarcharType.createVarcharType(viewData.length());
        domains.put(new ThriftColumnHandle("$view", viewDataType, null, true),
                    Domain.singleValue(viewDataType,viewData));

        return new ThriftTableLayoutHandle(layout.getSchemaName(),
                layout.getSchemaName(),
                layout.getColumns(),
                TupleDomain.withColumnDomains(domains));
    }
}