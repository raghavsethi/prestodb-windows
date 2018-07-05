package io.panyu.skydrill.connector.thrift;

import com.facebook.presto.connector.thrift.ThriftConnector;
import com.facebook.presto.connector.thrift.ThriftIndexProvider;
import com.facebook.presto.connector.thrift.ThriftPageSourceProvider;
import com.facebook.presto.connector.thrift.ThriftSessionProperties;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.airlift.bootstrap.LifeCycleManager;

import javax.inject.Inject;

public class SkydrillThriftConnector
        extends ThriftConnector
{
    private final SkydrillThriftMetadata metadata;
    private final SkydrillThriftSplitManager splitManager;

    @Inject
    public SkydrillThriftConnector(LifeCycleManager lifeCycleManager,
                                   SkydrillThriftMetadata metadata,
                                   SkydrillThriftSplitManager splitManager,
                                   ThriftPageSourceProvider pageSourceProvider,
                                   ThriftSessionProperties sessionProperties,
                                   ThriftIndexProvider indexProvider) {
        super(lifeCycleManager, metadata, splitManager, pageSourceProvider, sessionProperties, indexProvider);
        this.metadata = metadata;
        this.splitManager = splitManager;
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

}
