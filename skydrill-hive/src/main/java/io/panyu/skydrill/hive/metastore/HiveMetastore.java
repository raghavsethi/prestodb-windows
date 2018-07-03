package io.panyu.skydrill.hive.metastore;

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.file.DatabaseMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.Identity;
import io.airlift.json.JsonCodec;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;

public class HiveMetastore {
    private static final String PRESTO_SCHEMA_FILE_NAME = ".prestoSchema";
    private final JsonCodec<DatabaseMetadata> databaseCodec = JsonCodec.jsonCodec(DatabaseMetadata.class);

    private final Path catalogDirectory;
    private final FileSystem metadataFileSystem;

    public HiveMetastore(HdfsEnvironment hdfsEnvironment,
                         String catalogDirectory,
                         String user)
    {
        this.catalogDirectory = new Path(catalogDirectory);
        try {
            metadataFileSystem = hdfsEnvironment.getFileSystem(
                    new HdfsEnvironment.HdfsContext(new Identity(user, Optional.empty())),
                    this.catalogDirectory);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    public  void writeDatabaseSchemaFile(Database database, Path directory, boolean overwrite)
    {
        Path schemaPath = new Path(directory, PRESTO_SCHEMA_FILE_NAME);
        writeFile("database schema", schemaPath, databaseCodec, new DatabaseMetadata(database), overwrite);
    }

    private <T> void writeFile(String type, Path path, JsonCodec<T> codec, T value, boolean overwrite)
    {
        try {
            byte[] json = codec.toJsonBytes(value);

            if (!overwrite) {
                if (metadataFileSystem.exists(path)) {
                    throw new PrestoException(HIVE_METASTORE_ERROR, type + " file already exists");
                }
            }

            metadataFileSystem.mkdirs(path.getParent());

            try (OutputStream outputStream = metadataFileSystem.create(path, overwrite)) {
                outputStream.write(json);
            }
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Could not write " + type, e);
        }
    }
}
