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
package io.panyu.skydrill.plugin.jdbc;

import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class JdbcConnectorFactory
        extends com.facebook.presto.plugin.jdbc.JdbcConnectorFactory
{
    private final Module module;
    private final ClassLoader classLoader;

    public JdbcConnectorFactory(String name, Module module, ClassLoader classLoader) {
        super(name, module, classLoader);
        this.module = module;
        this.classLoader = classLoader;
    }

    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfig, ConnectorContext context) {
        requireNonNull(requiredConfig, "requiredConfig is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(new JdbcModule(connectorId), module);

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            return injector.getInstance(JdbcConnector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
