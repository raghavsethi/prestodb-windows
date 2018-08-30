package io.panyu.skydrill.hive;

import com.facebook.presto.hive.HiveMetadataFactory;
import com.facebook.presto.hive.HiveTransactionManager;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.session.PropertyMetadata;
import io.airlift.bootstrap.LifeCycleManager;

import javax.inject.Inject;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class HiveConnector
        extends com.facebook.presto.hive.HiveConnector
{
    private final Set<Procedure> procedures;

    @Inject
    public HiveConnector(LifeCycleManager lifeCycleManager,
                         HiveMetadataFactory metadataFactory,
                         HiveTransactionManager transactionManager,
                         ConnectorSplitManager splitManager,
                         ConnectorPageSourceProvider pageSourceProvider,
                         ConnectorPageSinkProvider pageSinkProvider,
                         ConnectorNodePartitioningProvider nodePartitioningProvider,
                         Set<SystemTable> systemTables,
                         List<PropertyMetadata<?>> sessionProperties,
                         List<PropertyMetadata<?>> schemaProperties,
                         List<PropertyMetadata<?>> tableProperties,
                         ConnectorAccessControl accessControl,
                         ClassLoader classLoader,
                         Set<Procedure> procedures) {
        super(lifeCycleManager,
                metadataFactory,
                transactionManager,
                splitManager,
                pageSourceProvider,
                pageSinkProvider,
                nodePartitioningProvider,
                systemTables,
                procedures,
                sessionProperties,
                schemaProperties,
                tableProperties,
                accessControl,
                classLoader);
        this.procedures = requireNonNull(procedures, "procedures is null");
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }
}
