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

import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface JdbcClient
    extends com.facebook.presto.plugin.jdbc.JdbcClient
{
    List<SchemaTableName> getTableNames(ConnectorSession session, @Nullable String schema);
    JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName);

    default void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace) {
    }

    default void dropView(ConnectorSession session, SchemaTableName viewName) {
    }

    default List<SchemaTableName> listViews(ConnectorSession session, String schema) {
        return ImmutableList.of();
    }

    default Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix) {
        return ImmutableMap.of();
    }

    default Optional<ViewDefinition> getViewDefinition(SchemaTableName viewName) {
        return Optional.empty();
    }
}
