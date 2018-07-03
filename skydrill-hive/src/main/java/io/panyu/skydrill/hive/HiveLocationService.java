package io.panyu.skydrill.hive;

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.LocationHandle;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;

import javax.inject.Inject;

import java.io.IOException;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
import static com.facebook.presto.hive.HiveWriteUtils.createTemporaryPath;
import static com.facebook.presto.hive.HiveWriteUtils.getTableDefaultLocation;
import static com.facebook.presto.hive.HiveWriteUtils.pathExists;
import static com.facebook.presto.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY;
import static com.facebook.presto.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static com.facebook.presto.hive.LocationHandle.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY;
import static java.lang.String.format;

public class HiveLocationService
        extends com.facebook.presto.hive.HiveLocationService
{
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public HiveLocationService(HdfsEnvironment hdfsEnvironment) {
        super(hdfsEnvironment);
        this.hdfsEnvironment = hdfsEnvironment;
    }

    @Override
    public LocationHandle forNewTable(SemiTransactionalHiveMetastore metastore, ConnectorSession session, String schemaName, String tableName)
    {
        HdfsEnvironment.HdfsContext context = new HdfsEnvironment.HdfsContext(session, schemaName, tableName);
        Path targetPath = getTableDefaultLocation(context, metastore, hdfsEnvironment, schemaName, tableName);

        // verify the target directory for the table
        if (pathExists(context, hdfsEnvironment, targetPath)) {
            throw new PrestoException(HIVE_PATH_ALREADY_EXISTS, format("Target directory for table '%s.%s' already exists: %s", schemaName, tableName, targetPath));
        }

        if (shouldUseTemporaryDirectory(context, targetPath)) {
            Path writePath = createTemporaryPath(context, hdfsEnvironment, targetPath);
            return new LocationHandle(targetPath, writePath, false, STAGE_AND_MOVE_TO_TARGET_DIRECTORY);
        }
        else {
            return new LocationHandle(targetPath, targetPath, false, DIRECT_TO_TARGET_NEW_DIRECTORY);
        }
    }

    @Override
    public LocationHandle forExistingTable(SemiTransactionalHiveMetastore metastore, ConnectorSession session, Table table)
    {
        HdfsEnvironment.HdfsContext context = new HdfsEnvironment.HdfsContext(session, table.getDatabaseName(), table.getTableName());
        Path targetPath = new Path(table.getStorage().getLocation());

        if (shouldUseTemporaryDirectory(context, targetPath)) {
            Path writePath = createTemporaryPath(context, hdfsEnvironment, targetPath);
            return new LocationHandle(targetPath, writePath, true, STAGE_AND_MOVE_TO_TARGET_DIRECTORY);
        }
        else {
            return new LocationHandle(targetPath, targetPath, true, DIRECT_TO_TARGET_EXISTING_DIRECTORY);
        }
    }

    private boolean shouldUseTemporaryDirectory(HdfsEnvironment.HdfsContext context, Path path)
    {
        return !(isNativeAzureFileSystem(context, hdfsEnvironment, path) ||
                 isAdlFileSystem(context, hdfsEnvironment, path));
    }

    private static boolean isNativeAzureFileSystem(HdfsEnvironment.HdfsContext context,
                                                  HdfsEnvironment hdfsEnvironment,
                                                  Path path)
    {
        try {
            return getRawFileSystem(hdfsEnvironment.getFileSystem(context, path)) instanceof NativeAzureFileSystem;
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed checking path: " + path, e);
        }
    }

    private static boolean isAdlFileSystem(HdfsEnvironment.HdfsContext context,
                                                   HdfsEnvironment hdfsEnvironment,
                                                   Path path)
    {
        try {
            return getRawFileSystem(hdfsEnvironment.getFileSystem(context, path))
                    .getClass().getName().equals("org.apache.hadoop.fs.adl.AdlFileSystem");
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed checking path: " + path, e);
        }
    }

    private static FileSystem getRawFileSystem(FileSystem fileSystem)
    {
        if (fileSystem instanceof FilterFileSystem) {
            return getRawFileSystem(((FilterFileSystem) fileSystem).getRawFileSystem());
        }
        return fileSystem;
    }
}
