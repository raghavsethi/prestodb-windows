package io.panyu.skydrill.hive;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.panyu.skydrill.hive.metastore.AdlsMetastoreModule;
import io.panyu.skydrill.hive.metastore.AzureBlobMetastoreModule;

import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static java.util.Objects.requireNonNull;

public class HiveMetastoreModule
        extends AbstractConfigurationAwareModule
{
    private final String connectorId;

    public HiveMetastoreModule(String connectorId) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    protected void setup(Binder binder) {
        bindMetastoreModule("adls", new AdlsMetastoreModule(connectorId));
        bindMetastoreModule("blob", new AzureBlobMetastoreModule(connectorId));
    }

    private void bindMetastoreModule(String name, Module module)
    {
        install(installModuleIf(
                HiveMetastoreConfig.class,
                metastore -> name.equalsIgnoreCase(metastore.getMetastoreType()),
                module));
    }
}
