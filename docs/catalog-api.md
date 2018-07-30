## **Dynamic Catalog APIs**

The Dynamic Catalog REST APIs allows catalogs to be loaded and dropped at runtime. Once a catalog is loaded, it is added to the catalog store and included in the subsequent service bootstrap loading.

### Listing Catalogs

Make a GET request to /v1/cat/list to get a list of loaded catalogs. Returns status 200 and a JSON array containing name of the catalogs.

Example:

    curl http://localhost:8080/v1/cat/list
    ["adls","hive","stripe","system","tpch"]

### Loading Catalog

Make a POST request to /v1/cat/load with the name of the new catalog and content in the request body. Returns status 200 and the path of the catalog if successful.

Example:

    curl --data-binary @tpcds.properties http://localhost:8080/v1/cat/load?catalog=tpcds
    /skydrill/runtime/catalog/tpcds

Content of `tpcds.properties`

    connector.name=tpcds

### Dropping Catalog

Make a DELETE request to /v1/cat/drop with the name of the catalog. Returns status 200 if successful.

Example:

    curl -X DELETE http://localhost:8080/v1/cat/drop?catalog=tpcds
