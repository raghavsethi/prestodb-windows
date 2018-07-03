package io.panyu.skydrill.hive.metastore;

import com.facebook.presto.hive.ForCachingHiveMetastore;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class AdlsMetastoreModule
        implements Module
{
    private final String connectorId;

    public AdlsMetastoreModule(String connectorId) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    public void configure(Binder binder) {
        configBinder(binder).bindConfig(AdlsMetastoreConfig.class);
        binder.bind(FileHiveMetastore.class).to(AdlsMetastore.class).in(Scopes.SINGLETON);
        binder.bind(ExtendedHiveMetastore.class).annotatedWith(ForCachingHiveMetastore.class).to(FileHiveMetastore.class);
        binder.bind(ExtendedHiveMetastore.class).to(CachingHiveMetastore.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ExtendedHiveMetastore.class)
                .as(generatedNameOf(CachingHiveMetastore.class, connectorId));

    }
}
