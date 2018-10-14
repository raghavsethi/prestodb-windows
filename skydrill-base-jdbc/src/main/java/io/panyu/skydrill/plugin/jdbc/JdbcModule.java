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

import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.plugin.jdbc.JdbcRecordSetProvider;
import com.facebook.presto.plugin.jdbc.JdbcSplitManager;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class JdbcModule
        implements Module
{
    private final String connectorId;

    public JdbcModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
    }

    @Override
    public void configure(Binder binder)
    {
        newOptionalBinder(binder, ConnectorAccessControl.class);
        binder.bind(com.facebook.presto.plugin.jdbc.JdbcClient.class).to(JdbcClient.class);
        binder.bind(JdbcConnectorId.class).toInstance(new JdbcConnectorId(connectorId));
        binder.bind(JdbcMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcConnector.class).in(Scopes.SINGLETON);
        binder.bind(JdbcSessionProperties.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(JdbcMetadataConfig.class);
    }
}
