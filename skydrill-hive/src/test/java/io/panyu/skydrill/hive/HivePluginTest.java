package io.panyu.skydrill.hive;

import com.facebook.presto.hive.HiveConnector;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.ServiceLoader;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.*;

@SuppressWarnings("unchecked")
public class HivePluginTest {

    @Test
    public void testPlugin()
    {
        HivePlugin plugin = loadPlugin(HivePlugin.class);

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, HiveConnectorFactory.class);

        Map<String, String> config = ImmutableMap.of("hive.metastore.container", "foo@bar");

        Connector connector = factory.create("test", config, new TestingConnectorContext());
        assertNotNull(connector);
        assertInstanceOf(connector, HiveConnector.class);
    }

    private static <T extends Plugin> T loadPlugin(Class<T> clazz)
    {
        for (Plugin plugin : ServiceLoader.load(Plugin.class)) {
            if (clazz.isInstance(plugin)) {
                return (T) plugin;
            }
        }
        throw new AssertionError("did not find plugin: " + clazz.getName());
    }
}