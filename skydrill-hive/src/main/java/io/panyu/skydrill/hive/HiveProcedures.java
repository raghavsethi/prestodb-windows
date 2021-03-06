package io.panyu.skydrill.hive;

import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.lang.invoke.MethodHandle;
import java.util.HashSet;
import java.util.Set;

import static com.facebook.presto.spi.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static java.util.Objects.requireNonNull;

public class HiveProcedures {
    private static final Logger log = Logger.get(HiveProcedures.class);
    private static final MethodHandle ADD_PARTITION = methodHandle(HiveProcedures.class, "addPartition", String.class, String.class, String.class);
    private static final MethodHandle ADD_PARTITIONS = methodHandle(HiveProcedures.class, "addPartitions", String.class, String.class, String.class, String.class,  String.class);
    private static final MethodHandle DROP_PARTITION = methodHandle(HiveProcedures.class, "dropPartition", String.class, String.class, String.class);

    private final ClassLoader classLoader;
    private final ExtendedHiveMetastore metastore;
    private final HivePartitionUtils utils;
    private final Set<Procedure> procedures;

    @Inject
    public HiveProcedures(ExtendedHiveMetastore metastore,
                          ClassLoader classLoader) {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.utils = new HivePartitionUtils();
        this.procedures = new HashSet<>();
    }

    public void addPartition(String databaseName, String tableName, String partitionValue) {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            utils.addPartition(metastore, databaseName, tableName, partitionValue);
            log.info("added partition: %s %s %s", databaseName, tableName, partitionValue);
        }
    }

    public void addPartitions(String databaseName, String tableName, String startDateTime, String endDateTime, String format) {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            utils.addPartitions(metastore, databaseName, tableName, startDateTime, endDateTime, format);
            log.info("added partitions: %s %s %s %s", databaseName, tableName, startDateTime, endDateTime);
        }
    }

    public void dropPartition(String databaseName, String tableName, String partitionValue) {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            utils.dropPartition(metastore, databaseName, tableName, partitionValue);
            log.info("dropped partition: %s %s %s", databaseName, tableName, partitionValue);
        }
    }

    HiveProcedures addProcedure(Procedure procedure) {
        procedures.add(procedure);
        return this;
    }

    Set<Procedure> getProcedures() {
        procedures.add(
                new Procedure("schema", "add_partition",
                        ImmutableList.<Procedure.Argument>builder()
                                .add(new Procedure.Argument("database", VARCHAR))
                                .add(new Procedure.Argument("table", VARCHAR))
                                .add(new Procedure.Argument("partition", VARCHAR))
                                .build(),
                        ADD_PARTITION.bindTo(this)));
        procedures.add(
                new Procedure("schema", "add_partitions",
                        ImmutableList.<Procedure.Argument>builder()
                                .add(new Procedure.Argument("database", VARCHAR))
                                .add(new Procedure.Argument("table", VARCHAR))
                                .add(new Procedure.Argument("startDateTime", VARCHAR))
                                .add(new Procedure.Argument("endDateTime", VARCHAR))
                                .add(new Procedure.Argument("format", VARCHAR))
                                .build(),
                        ADD_PARTITIONS.bindTo(this)));
        procedures.add(
                new Procedure("schema", "drop_partition",
                        ImmutableList.<Procedure.Argument>builder()
                                .add(new Procedure.Argument("database", VARCHAR))
                                .add(new Procedure.Argument("table", VARCHAR))
                                .add(new Procedure.Argument("partition", VARCHAR))
                                .build(),
                        DROP_PARTITION.bindTo(this)));
        return ImmutableSet.copyOf(procedures);
    }
}
