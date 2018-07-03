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

import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.panyu.skydrill.plugin.jdbc.metastore.JdbcMetastore;
import io.panyu.skydrill.plugin.jdbc.metastore.ZkJdbcMetastore;
import io.panyu.skydrill.plugin.jdbc.metastore.ZkJdbcMetastoreConfig;
import org.h2.Driver;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.String.format;

class TestingH2JdbcModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(com.facebook.presto.plugin.jdbc.JdbcClient.class).to(JdbcClient.class);
        binder.bind(JdbcMetastore.class).to(ZkJdbcMetastore.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(ZkJdbcMetastoreConfig.class);
    }

    @Provides
    public JdbcClient provideJdbcClient(JdbcConnectorId id, BaseJdbcConfig config, JdbcMetastore metastore)
    {
        return new BaseJdbcClient(id, config, "\"", new DriverConnectionFactory(new Driver(), config), metastore);
    }

    public static Map<String, String> createProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("connection-url", format("jdbc:h2:mem:test%s;DB_CLOSE_DELAY=-1", System.nanoTime()))
                .build();
    }
}
