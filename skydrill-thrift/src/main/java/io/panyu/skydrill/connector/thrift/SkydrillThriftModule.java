package io.panyu.skydrill.connector.thrift;

import com.facebook.presto.connector.thrift.ThriftConnector;
import com.facebook.presto.connector.thrift.ThriftConnectorConfig;
import com.facebook.presto.connector.thrift.ThriftConnectorStats;
import com.facebook.presto.connector.thrift.ThriftIndexProvider;
import com.facebook.presto.connector.thrift.ThriftMetadata;
import com.facebook.presto.connector.thrift.ThriftPageSourceProvider;
import com.facebook.presto.connector.thrift.ThriftSessionProperties;
import com.facebook.presto.connector.thrift.ThriftSplitManager;
import com.facebook.presto.connector.thrift.annotations.ForMetadataRefresh;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftServiceException;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.drift.client.ExceptionClassification;

import io.panyu.skydrill.plugin.jdbc.metastore.JdbcMetastore;
import io.panyu.skydrill.plugin.jdbc.metastore.ZkJdbcMetastore;
import io.panyu.skydrill.plugin.jdbc.metastore.ZkJdbcMetastoreConfig;

import java.util.Optional;
import java.util.concurrent.Executor;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.drift.client.ExceptionClassification.NORMAL_EXCEPTION;
import static io.airlift.drift.client.guice.DriftClientBinder.driftClientBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class SkydrillThriftModule
        implements Module {

    private final String connectorId;
    public SkydrillThriftModule(String connectorId) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(JdbcConnectorId.class).toInstance(new JdbcConnectorId(connectorId));
        binder.bind(SkydrillThriftClient.class).in(Scopes.SINGLETON);
        binder.bind(JdbcMetastore.class).to(ZkJdbcMetastore.class).in(Scopes.SINGLETON);
        //configBinder(binder).bindConfig(SkydrillThriftConfig.class);
        configBinder(binder).bindConfig(ZkJdbcMetastoreConfig.class);

        driftClientBinder(binder)
                .bindDriftClient(PrestoThriftService.class)
                .withExceptionClassifier(t -> {
                    if (t instanceof PrestoThriftServiceException) {
                        boolean retryable = ((PrestoThriftServiceException) t).isRetryable();
                        return new ExceptionClassification(Optional.of(retryable), ExceptionClassification.HostStatus.NORMAL);
                    }
                    return NORMAL_EXCEPTION;
                });

        binder.bind(ThriftConnector.class).to(SkydrillThriftConnector.class);
        binder.bind(ThriftMetadata.class).to(SkydrillThriftMetadata.class);
        binder.bind(ThriftSplitManager.class).to(SkydrillThriftSplitManager.class);
        binder.bind(SkydrillThriftConnector.class).in(Scopes.SINGLETON);
        binder.bind(SkydrillThriftMetadata.class).in(Scopes.SINGLETON);
        binder.bind(SkydrillThriftSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ThriftPageSourceProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ThriftConnectorConfig.class);
        binder.bind(ThriftSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(ThriftIndexProvider.class).in(Scopes.SINGLETON);
        binder.bind(ThriftConnectorStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ThriftConnectorStats.class)
                .as(generatedNameOf(ThriftConnectorStats.class, connectorId));
    }

    @Provides
    @Singleton
    @ForMetadataRefresh
    public Executor createMetadataRefreshExecutor(ThriftConnectorConfig config)
    {
        return newFixedThreadPool(config.getMetadataRefreshThreads(), daemonThreadsNamed("metadata-refresh-%s"));
    }
}
