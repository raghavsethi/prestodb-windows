package io.panyu.skydrill.plugin.jdbc;

import com.facebook.presto.plugin.jdbc.JdbcRecordSetProvider;
import com.facebook.presto.plugin.jdbc.JdbcSplitManager;
import io.airlift.bootstrap.LifeCycleManager;

import javax.inject.Inject;

public class JdbcConnector
        extends com.facebook.presto.plugin.jdbc.JdbcConnector
{
    @Inject
    public JdbcConnector(LifeCycleManager lifeCycleManager,
                         JdbcMetadataFactory jdbcMetadataFactory,
                         JdbcSplitManager jdbcSplitManager,
                         JdbcRecordSetProvider jdbcRecordSetProvider,
                         JdbcPageSinkProvider jdbcPageSinkProvider) {
        super(lifeCycleManager, jdbcMetadataFactory, jdbcSplitManager, jdbcRecordSetProvider, jdbcPageSinkProvider);
    }
}
