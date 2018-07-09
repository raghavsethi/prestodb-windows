package io.panyu.skydrill.connector.thrift;

import com.facebook.presto.connector.thrift.ThriftConnector;
import com.facebook.presto.connector.thrift.ThriftIndexProvider;
import com.facebook.presto.connector.thrift.ThriftPageSourceProvider;
import com.facebook.presto.connector.thrift.ThriftSessionProperties;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import io.airlift.bootstrap.LifeCycleManager;

import javax.inject.Inject;
import java.util.List;

public class SkydrillThriftConnector
        extends ThriftConnector
{
    private final SkydrillThriftMetadata metadata;
    private final SkydrillThriftSplitManager splitManager;
    private final SkydrillThriftSessionProperties sessionProperties;

    @Inject
    public SkydrillThriftConnector(LifeCycleManager lifeCycleManager,
                                   SkydrillThriftMetadata metadata,
                                   SkydrillThriftSplitManager splitManager,
                                   ThriftPageSourceProvider pageSourceProvider,
                                   ThriftSessionProperties properties,
                                   SkydrillThriftSessionProperties sessionProperties,
                                   ThriftIndexProvider indexProvider) {
        super(lifeCycleManager, metadata, splitManager, pageSourceProvider, properties, indexProvider);
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.sessionProperties = sessionProperties;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties.getSessionProperties();
    }

}
