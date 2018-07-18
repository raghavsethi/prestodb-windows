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
package io.panyu.skydrill.metadata;

import io.airlift.configuration.Config;

public class CatalogStoreConfig {
    private boolean coordinator = true;
    private String catalogRootPath = "/skydrill/runtime/catalog";

    public boolean isCoordinator()
    {
        return coordinator;
    }

    @Config("coordinator")
    public CatalogStoreConfig setCoordinator(boolean coordinator)
    {
        this.coordinator = coordinator;
        return this;
    }

    public String getCatalogRootPath() {
        return catalogRootPath;
    }

    @Config("runtime.catalog.root-path")
    public CatalogStoreConfig setCatalogRootPath(String rootPath) {
        this.catalogRootPath = rootPath;
        return this;
    }
}
