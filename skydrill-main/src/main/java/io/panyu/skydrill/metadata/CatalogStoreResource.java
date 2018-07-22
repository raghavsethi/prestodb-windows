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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.airlift.log.Logger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/v1/cat")
public class CatalogStoreResource {

    private static final Logger log = Logger.get(CatalogStoreResource.class);
    private final CuratorFramework curator;
    private final CatalogStoreConfig config;
    private final CatalogStore catalogStore;

    @Inject
    public CatalogStoreResource(CuratorFramework curator,
                                CatalogStoreConfig config,
                                CatalogStore catalogStore)
    {
        this.curator = requireNonNull(curator, "curator is null");
        this.config = requireNonNull(config, "CatalogStoreConfig is null");
        this.catalogStore = requireNonNull(catalogStore, "catalogStore is null");
    }

    private String toValidJSON(final String input, ObjectMapper mapper) throws Exception
    {
        if (Strings.isNullOrEmpty(input))
            return null;

        mapper = (mapper == null)? new ObjectMapper() : mapper;
        try {
            mapper.readTree(input);
            return input;
        } catch (JsonParseException e) {
            return mapper.writeValueAsString(input);
        }
    }

    @GET
    @Path("children")
    @Produces(APPLICATION_JSON)
    public List<String> children(@QueryParam("path") String path) throws Exception
    {
        return curator.getChildren().forPath(path).stream()
                .sorted()
                .collect(Collectors.toList());
    }

    @GET
    @Path("node")
    @Produces(APPLICATION_JSON)
    public Response node(@QueryParam("path") String path) throws Exception
    {
        org.apache.zookeeper.data.Stat stat = new Stat();
        Optional<byte[]> bytes = Optional.ofNullable(curator.getData().storingStatIn(stat).forPath(path));
        ObjectMapper mapper = new ObjectMapper();

        String json = "{\"data\":" +
                toValidJSON(bytes.map(String::new).orElse(null), mapper) +
                ",\"stat\":" +
                mapper.writeValueAsString(stat) +
                "}";

        Response.ResponseBuilder response = Response.ok(json);
        return response.build();
    }

    @POST
    @Path("load")
    @Produces(APPLICATION_JSON)
    public Response loadCatalog(String content,
                            @QueryParam("catalog") String catalog) throws Exception
    {
        requireNonNull(catalog, "catalog is null");
        requireNonNull(content, "content id null");
        Response.ResponseBuilder response;

        String catalogPath = String.format("%s/%s", config.getCatalogRootPath(), catalog);
        if (curator.checkExists().forPath(catalogPath) == null) {
            String s = curator.create().forPath(catalogPath, content.getBytes());
            catalogStore.loadCatalog(catalog, true);
            response = Response.ok(s);
        } else {
            response = Response.status(409, "exists");
        }
        return response.build();
    }

    @DELETE
    @Path("drop")
    @Produces(APPLICATION_JSON)
    public Response dropCatalog(@QueryParam("catalog") String catalog) throws Exception
    {
        requireNonNull(catalog, "catalog is null");
        Response.ResponseBuilder response;

        String catalogPath = String.format("%s/%s", config.getCatalogRootPath(), catalog);
        if (curator.checkExists().forPath(catalogPath) == null) {
            response = Response.status(404, "not found");
        } else {
            catalogStore.dropCatalog(catalog, true);
            curator.delete().forPath(catalogPath);
            response = (curator.checkExists().forPath(catalogPath) == null)? Response.ok() :
                    Response.status(500, "delete catalog failed");
        }
        return response.build();
    }

    @GET
    @Path("list")
    @Produces(APPLICATION_JSON)
    public List<String> getCatalogs()
    {
        return catalogStore.getCatalogs().stream()
                .sorted()
                .collect(Collectors.toList());
    }
}
