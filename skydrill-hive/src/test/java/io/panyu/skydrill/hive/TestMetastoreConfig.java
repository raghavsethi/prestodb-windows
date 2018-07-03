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
package io.panyu.skydrill.hive;

import com.google.common.collect.ImmutableMap;
import io.panyu.skydrill.hive.metastore.AdlsMetastoreConfig;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.*;

public class TestMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HiveMetastoreConfig.class)
                .setMetastoreType("blob")
                .setMetastoreUser("skydrill"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore", "adls")
                .put("adl.metastore.account", "myacct")
                .put("hive.metastore.catalog.dir", "/cat")
                .put("hive.metastore.user", "presto")
                .build();

        AdlsMetastoreConfig expected = new AdlsMetastoreConfig();
        expected.setMetastoreType("adls");
        expected.setAccount("myacct");
        expected.setCatalogDirectory("/cat");
        expected.setMetastoreUser("presto");

        assertFullMapping(properties, expected);
    }

}