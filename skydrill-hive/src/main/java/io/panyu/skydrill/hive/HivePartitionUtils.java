package io.panyu.skydrill.hive;

import com.facebook.presto.hive.ForCachingHiveMetastore;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveConnectorId;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.authentication.HdfsAuthentication;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.authentication.NoHiveMetastoreAuthentication;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.HiveMetastoreClientFactory;
import com.facebook.presto.hive.metastore.thrift.StaticHiveCluster;
import com.facebook.presto.hive.metastore.thrift.StaticMetastoreConfig;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastore;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.hive.s3.S3ConfigurationUpdater;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.panyu.skydrill.hive.metastore.*;

import javax.inject.Singleton;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.collect.Maps.fromProperties;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class HivePartitionUtils {
    private static final Logger log = Logger.get(HivePartitionUtils.class);
    private static final Splitter nSplitter = Splitter.on(' ').trimResults().omitEmptyStrings();
    private static final Splitter pSplitter = Splitter.on('/').trimResults().omitEmptyStrings();
    private static final Splitter vSplitter = Splitter.on('=').trimResults().omitEmptyStrings();
    private static final Splitter nsSplitter = Splitter.on('.').trimResults().omitEmptyStrings();

    public static void main(String[] args)
    {
        if (args.length < 2) {
            printUsage();
            return;
        }

        String cmd = args[0];
        String skydrillHome = System.getProperty("skydrill.home");
        requireNonNull(skydrillHome, "skydrill.home property not set");

        List<String> fqTableName = nsSplitter.splitToList(args[1]);
        if (fqTableName.size() != 3) {
            throw new RuntimeException(args[1] + " is not a fully qualified table name");
        }

        String catalog = fqTableName.get(0);
        String databaseName = fqTableName.get(1);
        String tableName = fqTableName.get(2);

        String catalogFile = String.format("%s/etc/catalog/%s.properties", skydrillHome, catalog);
        if (Files.notExists(Paths.get(catalogFile))) {
            throw new RuntimeException(catalogFile + " catalog file does not exist");
        }

        try {
            HivePartitionUtils utils = new HivePartitionUtils();
            ExtendedHiveMetastore store = utils.getMetastoreByCatalog(catalogFile);

            switch (cmd) {
                case "add" : {
                    if (args.length < 3) {
                        log.error("missing partition value");
                        printUsage();
                        return;
                    }

                    String partitionValue = args[2];
                    if (partitionValue.startsWith("@")) {
                        utils.addPartitions(store, databaseName, tableName,
                                Files.readAllLines(Paths.get(partitionValue.substring(1))));
                        return;
                    }

                    partitionValue = (args.length == 4)? partitionValue + " " + args[3] : partitionValue;
                    utils.addPartition(store, databaseName, tableName, partitionValue);
                    log.info("added " + partitionValue);
                }
                break;

                case "update" : {
                    if (args.length < 4) {
                        log.error("missing partition value");
                        printUsage();
                        return;
                    }

                    String partitionValue = args[2];
                    String location = args[3];

                    utils.alterPartition(store, databaseName, tableName, partitionValue, location);
                    log.info("updated " + partitionValue); }
                break;

                case "drop": {
                    String partitionValue = args[2];
                    utils.dropPartition(store, databaseName, tableName, partitionValue);
                    log.info("dropped " + partitionValue); }
                break;

                case "list": {
                    utils.listPartitions(store, databaseName, tableName)
                            .forEach(x -> log.info("%s,%s%n", x,
                                    utils.getPartition(store, databaseName, tableName, utils.getNormalizedValue(x)))); }
                break;

                case "get":  {
                    String partitionValue = utils.getNormalizedValue(args[2]);
                    log.info(utils.getPartition(store, databaseName, tableName, partitionValue)); }
                break;

                default:
                    log.error("invalid cmd " + cmd);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static void printUsage()
    {
        System.out.println("usage: HivePartitionUtils [add|drop|list|update] catalog.schema.table [partition] [location]");
    }

    private ExtendedHiveMetastore createThriftMetastore(Map<String, String> config)
    {
        StaticMetastoreConfig metastoreConfig = new StaticMetastoreConfig();
        metastoreConfig.setMetastoreUris(config.get("hive.metastore.uri"));
        metastoreConfig.setMetastoreUsername(System.getProperty("user.name"));

        HiveMetastoreClientFactory clientFactory = new HiveMetastoreClientFactory(
                Optional.empty(),
                Optional.empty(),
                new Duration(60000, TimeUnit.MILLISECONDS),
                new NoHiveMetastoreAuthentication()
        );

        StaticHiveCluster hiveCluster = new StaticHiveCluster(metastoreConfig, clientFactory);
        return new BridgingHiveMetastore(new ThriftHiveMetastore(hiveCluster));
    }

    private ExtendedHiveMetastore createFileMetastore(Map<String, String> config)
    {
        HiveClientConfig hiveConfig = new HiveClientConfig();
        hiveConfig.setResourceConfigFiles(config.get("hive.config.resources"));
        S3ConfigurationUpdater s3updater = new PrestoS3ConfigurationUpdater(new HiveS3Config());
        HdfsConfigurationUpdater updater = new HdfsConfigurationUpdater(hiveConfig, s3updater);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(updater);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveConfig, new NoHdfsAuthentication());

        return new FileHiveMetastore(hdfsEnvironment,
                config.get("hive.metastore.catalog.dir"),
                System.getProperty("user.name"));
    }

    private ExtendedHiveMetastore createAzureBlobMetastore(String connectorId, Map<String, String> config)
            throws Exception {
        Bootstrap app = new Bootstrap(
                new MetastoreModule(connectorId),
                new AzureBlobMetastoreModule(connectorId),
                binder -> binder.bind(AzureBlobMetastore.class).in(Scopes.SINGLETON));

        Injector injector = app.setRequiredConfigurationProperties(config).initialize();
        return injector.getInstance(AzureBlobMetastore.class);
    }

    private ExtendedHiveMetastore createAdlsMetastore(String connectorId, Map<String, String> config)
            throws Exception {
        Bootstrap app = new Bootstrap(
                new MetastoreModule(connectorId),
                new AdlsMetastoreModule(connectorId),
                binder -> binder.bind(AdlsMetastore.class).in(Scopes.SINGLETON));

        Injector injector = app.setRequiredConfigurationProperties(config).initialize();
        return injector.getInstance(AdlsMetastore.class);
    }

    private ExtendedHiveMetastore getMetastoreByCatalog(String catalogProperties) throws Exception
    {
        Properties properties = new Properties();
        properties.load(new FileInputStream(catalogProperties));

        String connectorName = properties.getProperty("connector.name");
        if (!(connectorName.equals("skydrill-hive") || connectorName.equals("hive-hadoop2")))
            throw new RuntimeException("not a hive catalog");

        String type = connectorName.equals("skydrill-hive")?
                properties.getProperty("hive.metastore.type", "blob") :
                properties.getProperty("hive.metastore.type", "thrift");

        properties.remove("connector.name");
        String connectorId = nsSplitter.splitToList(catalogProperties).get(0);
        Map<String, String> config = ImmutableMap.copyOf(fromProperties(properties));

        switch (type) {
            case "blob" :
                return createAzureBlobMetastore(connectorId, config);
            case "adls" :
                return createAdlsMetastore(connectorId, config);
            case "file" :
                return createFileMetastore(config);
            case "thrift" :
                default:
                return createThriftMetastore(config);
        }
    }

    private void addPartitions(ExtendedHiveMetastore store,
                               String databaseName,
                               String tableName,
                               List<String> partitionEntries)
    {
        store.getTable(databaseName, tableName).ifPresent(table -> {
            final Storage seedStorage = table.getStorage();
            final List<Column> seedColumns = new ImmutableList.Builder<Column>()
                    .addAll(table.getDataColumns())
                    .addAll(table.getPartitionColumns())
                    .build();
            final Map<String, String> seedParameters = table.getParameters();

            store.addPartitions(
                    databaseName,
                    tableName,
                    partitionEntries
                        .stream()
                        .map(e -> makePartition(databaseName, tableName, e, seedStorage, seedColumns, seedParameters))
                        .collect(Collectors.toList()));
        });
    }

    private void addPartition(ExtendedHiveMetastore store,
                              String databaseName,
                              String tableName,
                              String partitionValue)
    {
        addPartitions(store, databaseName, tableName, Collections.singletonList(partitionValue));
    }
    
    private Storage makeStorage(Storage seed, String location)
    {
        return new Storage(
                seed.getStorageFormat(),
                location,
                seed.getBucketProperty(),
                seed.isSkewed(),
                seed.getSerdeParameters());
    }

    private Partition makePartition(String databaseName,
                                    String tableName,
                                    String partitionEntry,
                                    Storage seedStorage,
                                    List<Column> seedColumns,
                                    Map<String, String> seedParameters)
    {
        List<String> parts = nSplitter.splitToList(partitionEntry);
        return new Partition(
                databaseName,
                tableName,
                pSplitter.splitToList(getNormalizedValue(parts.get(0))),
                makeStorage(seedStorage, (parts.size() > 1)?
                        parts.get(1) :
                        String.format("%s/%s", seedStorage.getLocation(), parts.get(0))),
                seedColumns,
                seedParameters);
    }

    private void dropPartition(ExtendedHiveMetastore store,
                               String databaseName,
                               String tableName,
                               String partitionValue)
    {
        List<String> partitionValues = pSplitter.splitToList(getNormalizedValue(partitionValue));
        store.dropPartition(databaseName, tableName, partitionValues, true);
    }

    private List<String> listPartitions(ExtendedHiveMetastore store,
                                        String database,
                                        String table)
    {
        Optional<List<String>> partitions = store.getPartitionNames(database, table);
        return partitions.orElse(null);
    }

    private void alterPartition(ExtendedHiveMetastore store,
                                String databaseName,
                                String tableName,
                                String partitionValue,
                                String newLocation)
    {
        List<String> partitionValues = pSplitter.splitToList(getNormalizedValue(partitionValue));
        Optional<Partition> partition = store.getPartition(databaseName, tableName, partitionValues);
        if (!partition.isPresent()) {
            throw new RuntimeException(partitionValue + " partition does not exists");
        }

        Partition newPartition = new Partition(
                partition.get().getDatabaseName(),
                partition.get().getTableName(),
                partition.get().getValues(),
                new Storage(
                        partition.get().getStorage().getStorageFormat(),
                        newLocation,
                        partition.get().getStorage().getBucketProperty(),
                        partition.get().getStorage().isSkewed(),
                        partition.get().getStorage().getSerdeParameters()
                ),
                partition.get().getColumns(),
                partition.get().getParameters());

        store.alterPartition(databaseName, tableName, newPartition);
    }

    private String getPartition(ExtendedHiveMetastore store,
                                String database,
                                String table,
                                String partitionValue)
    {
        List<String> partitionValues = pSplitter.splitToList(getNormalizedValue(partitionValue));
        Optional<Partition> partition = store.getPartition(database, table, partitionValues);
        return partition.map(x -> x.getStorage()
                .getLocation())
                .orElseThrow(()-> new RuntimeException("failed to get partition" + partitionValue));
    }

    private String getNormalizedValue(String partitionValue)
    {
        List<String> parts = pSplitter.splitToList(partitionValue);
        return parts.stream()
                .map(x -> Iterables.getLast(vSplitter.splitToList(x)))
                .reduce((x, y) -> x + "/" + y)
                .orElseThrow(()-> new RuntimeException("fail to normalize " + partitionValue));
    }

    private class MetastoreModule implements Module {
        private final String connectorId;

        private MetastoreModule(String connectorId) {
            this.connectorId = connectorId;
        }

        @Override
        public void configure(Binder binder) {
            binder.bind(HiveConnectorId.class).toInstance(new HiveConnectorId(connectorId));
            binder.bind(com.facebook.presto.hive.HdfsConfigurationUpdater.class).to(HdfsConfigurationUpdater.class).in(Scopes.SINGLETON);
            binder.bind(HdfsConfiguration.class).to(HiveHdfsConfiguration.class).in(Scopes.SINGLETON);
            binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);
            binder.bind(HdfsAuthentication.class).to(NoHdfsAuthentication.class).in(Scopes.SINGLETON);
            binder.bind(S3ConfigurationUpdater.class).to(PrestoS3ConfigurationUpdater.class).in(Scopes.SINGLETON);
            binder.bind(com.facebook.presto.hive.HiveClientConfig.class).to(HiveClientConfig.class);
            configBinder(binder).bindConfig(HiveClientConfig.class);
            configBinder(binder).bindConfig(HiveS3Config.class);
        }

        @ForCachingHiveMetastore
        @Singleton
        @Provides
        public ExecutorService createCachingHiveMetastoreExecutor(HiveConnectorId hiveClientId,
                                                                  com.facebook.presto.hive.HiveClientConfig hiveClientConfig)
        {
            return newFixedThreadPool(
                    hiveClientConfig.getMaxMetastoreRefreshThreads(),
                    daemonThreadsNamed("hive-metastore-" + hiveClientId + "-%s"));
        }
    }

}
