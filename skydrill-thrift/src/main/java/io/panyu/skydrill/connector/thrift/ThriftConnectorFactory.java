package io.panyu.skydrill.connector.thrift;

import com.facebook.presto.connector.thrift.ThriftConnector;
import com.facebook.presto.connector.thrift.ThriftHandleResolver;
import com.facebook.presto.connector.thrift.ThriftModule;
import com.facebook.presto.connector.thrift.util.RebindSafeMBeanServer;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.drift.transport.netty.client.DriftNettyClientModule;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;
import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

public class ThriftConnectorFactory implements ConnectorFactory {
    private final ClassLoader classLoader;
    private final Module locationModule;

    public ThriftConnectorFactory(ClassLoader classLoader, Module locationModule) {
        this.classLoader = classLoader;
        this.locationModule = locationModule;
    }

    @Override
    public String getName() {
        return "skydrill-thrift";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new ThriftHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context) {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new MBeanModule(),
                    new DriftNettyClientModule(),
                    binder -> {
                        binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(getPlatformMBeanServer()));
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    },
                    locationModule,
                    new SkydrillThriftModule(connectorId));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(ThriftConnector.class);
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while creating connector", ie);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
