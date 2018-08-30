package io.panyu.skydrill.hive;

import com.facebook.presto.hive.CreateEmptyPartitionProcedure;
import com.facebook.presto.hive.HiveHandleResolver;
import com.facebook.presto.hive.HiveMetadataFactory;
import com.facebook.presto.hive.HiveSchemaProperties;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.HiveTableProperties;
import com.facebook.presto.hive.HiveTransactionManager;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.RebindSafeMBeanServer;
import com.facebook.presto.hive.authentication.HiveAuthenticationModule;
import com.facebook.presto.hive.s3.HiveS3Module;
import com.facebook.presto.hive.security.HiveSecurityModule;
import com.facebook.presto.hive.security.PartitionsAwareAccessControl;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorSplitManager;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeNodePartitioningProvider;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class HiveConnectorFactory
        implements ConnectorFactory
{
    private final ClassLoader classLoader;

    public HiveConnectorFactory(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public String getName() {
        return "skydrill-hive";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new HiveHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context) {
        requireNonNull(config, "config is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new EventModule(),
                    new MBeanModule(),
                    new JsonModule(),
                    new HiveClientModule(connectorId),
                    new HiveS3Module(connectorId),
                    new HiveMetastoreModule(connectorId),
                    new HiveSecurityModule(),
                    new HiveAuthenticationModule(),
                    binder -> {
                        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
                        binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(platformMBeanServer));
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                        binder.bind(PageSorter.class).toInstance(context.getPageSorter());
                        binder.bind(ClassLoader.class).toInstance(classLoader);
                        binder.bind(HiveProcedures.class).in(Scopes.SINGLETON);
                        binder.bind(CreateEmptyPartitionProcedure.class).in(Scopes.SINGLETON);
                    });

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            HiveMetadataFactory metadataFactory = injector.getInstance(HiveMetadataFactory.class);
            HiveTransactionManager transactionManager = injector.getInstance(HiveTransactionManager.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorPageSinkProvider pageSinkProvider = injector.getInstance(ConnectorPageSinkProvider.class);
            ConnectorNodePartitioningProvider connectorDistributionProvider = injector.getInstance(ConnectorNodePartitioningProvider.class);
            HiveSessionProperties hiveSessionProperties = injector.getInstance(HiveSessionProperties.class);
            HiveTableProperties hiveTableProperties = injector.getInstance(HiveTableProperties.class);
            ConnectorAccessControl accessControl = new PartitionsAwareAccessControl(injector.getInstance(ConnectorAccessControl.class));
            Procedure createEmptyPartitionProcedure = injector.getInstance(CreateEmptyPartitionProcedure.class).get();
            HiveProcedures hiveProcedures = injector.getInstance(HiveProcedures.class);
            Set<Procedure> procedureSet = hiveProcedures.addProcedure(createEmptyPartitionProcedure).getProcedures();

            return new HiveConnector(
                    lifeCycleManager,
                    metadataFactory,
                    transactionManager,
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
                    new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
                    ImmutableSet.of(),
                    hiveSessionProperties.getSessionProperties(),
                    HiveSchemaProperties.SCHEMA_PROPERTIES,
                    hiveTableProperties.getTableProperties(),
                    accessControl,
                    classLoader,
                    procedureSet);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
