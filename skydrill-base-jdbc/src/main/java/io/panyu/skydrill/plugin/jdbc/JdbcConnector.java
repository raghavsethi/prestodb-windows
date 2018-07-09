package io.panyu.skydrill.plugin.jdbc;

import com.facebook.presto.plugin.jdbc.JdbcRecordSetProvider;
import com.facebook.presto.plugin.jdbc.JdbcSplitManager;
import com.facebook.presto.spi.session.PropertyMetadata;
import io.airlift.bootstrap.LifeCycleManager;

import javax.inject.Inject;
import java.util.List;

public class JdbcConnector
        extends com.facebook.presto.plugin.jdbc.JdbcConnector
{
    private final JdbcSessionProperties jdbcSessionProperties;

    @Inject
    public JdbcConnector(LifeCycleManager lifeCycleManager,
                         JdbcMetadataFactory jdbcMetadataFactory,
                         JdbcSplitManager jdbcSplitManager,
                         JdbcRecordSetProvider jdbcRecordSetProvider,
                         JdbcPageSinkProvider jdbcPageSinkProvider,
                         JdbcSessionProperties jdbcSessionProperties) {
        super(lifeCycleManager, jdbcMetadataFactory, jdbcSplitManager, jdbcRecordSetProvider, jdbcPageSinkProvider);
        this.jdbcSessionProperties = jdbcSessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return jdbcSessionProperties.getSessionProperties();
    }
}
