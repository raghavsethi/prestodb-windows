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
package io.panyu.skydrill.server;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.airlift.log.Logger;
import io.panyu.skydrill.metadata.SkydrillCatalogStore;
import io.panyu.skydrill.metadata.SkydrillCatalogStoreConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/v1/cat")
public class SkydrillResource {
  private static final Logger log = Logger.get(SkydrillResource.class);
  private final CuratorFramework curator;
  private final SkydrillCatalogStoreConfig config;
  private final SkydrillCatalogStore catalogStore;

  @Inject
  public SkydrillResource(CuratorFramework curator,
                          SkydrillCatalogStoreConfig config,
                          SkydrillCatalogStore catalogStore) throws Exception
  {
    this.curator = requireNonNull(curator);
    this.config = requireNonNull(config);
    this.catalogStore = requireNonNull(catalogStore);
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
  @Path("data")
  @Produces(APPLICATION_JSON)
  public Response data(@QueryParam("path") String path) throws Exception
  {
    Optional<byte[]> bytes = Optional.ofNullable(curator.getData().forPath(path));
    String queryResults = bytes.map(String::new).orElse(null);
    Response.ResponseBuilder response = Response.ok(toValidJSON(queryResults, null));
    return response.build();
  }

  @GET
  @Path("stat")
  @Produces(APPLICATION_JSON)
  public Response stat(@QueryParam("path") String path) throws Exception
  {
    Optional<Stat> stat = Optional.ofNullable(curator.checkExists().forPath(path));
    ObjectMapper mapper = new ObjectMapper();
    String statString = mapper.writeValueAsString(stat.orElse(null));
    Response.ResponseBuilder response = Response.ok(statString);
    return response.build();
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

  @DELETE
  @Path("delete")
  @Produces(APPLICATION_JSON)
  public Response delete(@QueryParam("path") String path) throws Exception
  {
    curator.delete().forPath(path);
    boolean queryResults = curator.checkExists().forPath(path) == null;
    Response.ResponseBuilder response = Response.ok(queryResults);
    return response.build();
  }

  @POST
  @Path("install")
  @Produces(APPLICATION_JSON)
  public Response install(String content,
                      @QueryParam("catalog") String catalog) throws Exception
  {
    requireNonNull(catalog);
    requireNonNull(content);

    String path = String.format("%s/%s", config.getCatalogRootPath(), catalog);
    if (curator.checkExists().forPath(path) == null) {
      String s = curator.create().forPath(path, content.getBytes());
      catalogStore.loadCatalog(catalog);
      Response.ResponseBuilder response = Response.ok(s);
      return response.build();
    } else {
      Response.ResponseBuilder response = Response.status(400, "exists");
      return response.build();
    }
  }

  @DELETE
  @Path("uninstall")
  @Produces(APPLICATION_JSON)
  public Response uninstall(@QueryParam("catalog") String catalog) throws Exception
  {
    requireNonNull(catalog);
    String path = String.format("%s/%s", config.getCatalogRootPath(), catalog);
    curator.delete().forPath(path);
    boolean queryResults = curator.checkExists().forPath(path) == null;
    Response.ResponseBuilder response = Response.ok(queryResults);
    return response.build();
  }

  @GET
  @Path("list")
  @Produces(APPLICATION_JSON)
  public List<String> list() throws Exception
  {
    return curator.getChildren().forPath(config.getCatalogRootPath()).stream()
            .sorted()
            .collect(Collectors.toList());
  }
}
