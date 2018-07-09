/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.panyu.skydrill.connector.thrift;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertNotNull;

public class TestThriftPlugin
{
    @Test
    public void testPlugin()
    {
        System.setProperty("zookeeper.connect-string", "127.0.0.1:2181");
        Plugin plugin = new SkydrillThriftPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        Connector connector = factory.create("test",
                ImmutableMap.of("presto.thrift.client.addresses", "localhost:7779"),
                new TestingConnectorContext());
        assertNotNull(connector);
        assertInstanceOf(connector, SkydrillThriftConnector.class);
    }
}
