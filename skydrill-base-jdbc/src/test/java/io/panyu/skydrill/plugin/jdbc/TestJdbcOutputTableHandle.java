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

import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.panyu.skydrill.plugin.jdbc.MetadataUtil.OUTPUT_TABLE_CODEC;
import static io.panyu.skydrill.plugin.jdbc.MetadataUtil.assertJsonRoundTrip;

public class TestJdbcOutputTableHandle
{
    @Test
    public void testJsonRoundTrip()
    {
        JdbcOutputTableHandle handle = new JdbcOutputTableHandle(
                "connectorId",
                "catalog",
                "schema",
                "table",
                ImmutableList.of("abc", "xyz"),
                ImmutableList.of(VARCHAR, VARCHAR),
                "tmp_table");

        assertJsonRoundTrip(OUTPUT_TABLE_CODEC, handle);
    }
}
