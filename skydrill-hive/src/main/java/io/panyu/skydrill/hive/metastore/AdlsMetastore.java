package io.panyu.skydrill.hive.metastore;

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.SchemaAlreadyExistsException;
import com.facebook.presto.hive.metastore.Database;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class AdlsMetastore
        extends com.facebook.presto.hive.metastore.file.FileHiveMetastore
{
    private final HiveMetastore metastore;
    private final Path catalogDirectory;

    @Inject
    public AdlsMetastore(HdfsEnvironment hdfsEnvironment, AdlsMetastoreConfig config) {
        super(hdfsEnvironment, config.getCatalogDirectory(), config.getMetastoreUser());
        this.metastore = new HiveMetastore(hdfsEnvironment, config.getCatalogDirectory(), config.getMetastoreUser());
        this.catalogDirectory = new Path(config.getCatalogDirectory());
    }

    @Override
    public synchronized void createDatabase(Database database)
    {
        requireNonNull(database, "database is null");
        if (getDatabase(database.getDatabaseName()).isPresent()) {
            throw new SchemaAlreadyExistsException(database.getDatabaseName());
        }

        Path databaseMetadataDirectory = new Path(catalogDirectory, database.getDatabaseName());
        metastore.writeDatabaseSchemaFile(database, databaseMetadataDirectory, false);
    }

}
