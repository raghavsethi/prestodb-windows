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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import io.airlift.log.Logger;
import io.panyu.skydrill.metadata.CatalogStore;
import io.panyu.skydrill.metadata.CatalogStoreConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/v1/zk")
public class SkydrillResource {
  private static final Logger log = Logger.get(SkydrillResource.class);
  private final CuratorFramework curator;
  private final CatalogStoreConfig config;
  private final CatalogStore catalogStore;

  @Inject
  public SkydrillResource(CuratorFramework curator,
                          CatalogStoreConfig config,
                          CatalogStore catalogStore)
  {
    this.curator = requireNonNull(curator);
    this.config = requireNonNull(config);
    this.catalogStore = requireNonNull(catalogStore);
  }

  @GET
  @Path("get")
  @Produces(APPLICATION_JSON)
  public Response getData(@QueryParam("path") String path) throws Exception
  {
    Response.ResponseBuilder response;

    if (curator.checkExists().forPath(path) == null) {
      response = Response.status(404, "not found");
    } else {
      Optional<byte[]> bytes = Optional.ofNullable(curator.getData().forPath(path));
      response = Response.ok(bytes.map(x -> new String(x, Charsets.UTF_8)).orElse(""));
    }
    return response.build();
  }

  @POST
  @Path("set")
  @Produces(APPLICATION_JSON)
  public Response setData(String content, @QueryParam("path") String path) throws Exception
  {
    requireNonNull(path, "path is null");
    requireNonNull(content, "content id null");
    Response.ResponseBuilder response;

    if (curator.checkExists().forPath(path) == null) {
      response = Response.status(404, "not found");
    } else {
      curator.setData().forPath(path, content.getBytes());
      response = Response.ok(content.length());
    }
    return response.build();
  }

  @PUT
  @Path("add")
  @Produces(APPLICATION_JSON)
  public Response addData(String content, @QueryParam("path") String path) throws Exception
  {
    requireNonNull(path, "path is null");
    requireNonNull(content, "content id null");
    Response.ResponseBuilder response;

    if (curator.checkExists().forPath(path) == null) {
      curator.create().withMode(CreateMode.PERSISTENT).forPath(path, content.getBytes());
      response = Response.ok(content.length());
    } else {
      response = Response.status(409, "exists");
    }
    return response.build();
  }

  @GET
  @Path("stat")
  @Produces(APPLICATION_JSON)
  public Response stat(@QueryParam("path") String path) throws Exception
  {
    Response.ResponseBuilder response;

    Optional<Stat> stat = Optional.ofNullable(curator.checkExists().forPath(path));
    if (stat.isPresent()) {
      ObjectMapper mapper = new ObjectMapper();
      String statString = mapper.writeValueAsString(stat.get());
      response = Response.ok(statString);
    } else {
      response = Response.status(404, "not found");
    }
    return response.build();
  }

  @DELETE
  @Path("delete")
  @Produces(APPLICATION_JSON)
  public Response delete(@QueryParam("path") String path) throws Exception
  {
    Response.ResponseBuilder response;

    if (curator.checkExists().forPath(path) == null) {
      response = Response.status(404, "not found");
    } else {
      curator.delete().forPath(path);
      boolean queryResults = curator.checkExists().forPath(path) == null;
      response = Response.ok(queryResults);
    }
    return response.build();
  }

}
