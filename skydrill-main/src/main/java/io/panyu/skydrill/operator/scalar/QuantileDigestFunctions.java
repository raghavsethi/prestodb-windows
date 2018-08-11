package io.panyu.skydrill.operator.scalar;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.stats.QuantileDigest;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.Float.floatToRawIntBits;

public final class QuantileDigestFunctions {
    private QuantileDigestFunctions() {
    }

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bigint_percentile(@SqlType(StandardTypes.VARBINARY) Slice serializedDigest,
                                         @SqlType(StandardTypes.DOUBLE) double percentile) {
        return new QuantileDigest(serializedDigest).getQuantile(percentile);
    }

    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double percentile(@SqlType(StandardTypes.VARBINARY) Slice serializedDigest,
                                    @SqlType(StandardTypes.DOUBLE) double percentile) {
        return sortableLongToDouble(bigint_percentile(serializedDigest, percentile));
    }

    @ScalarFunction
    @SqlType(StandardTypes.REAL)
    public static int real_quantile(@SqlType(StandardTypes.VARBINARY) Slice serializedDigest,
                                   @SqlType(StandardTypes.DOUBLE) double percentile) {
        return floatToRawIntBits(sortableIntToFloat((int) bigint_percentile(serializedDigest, percentile)));
    }

    @ScalarFunction
    @SqlType("array(bigint)")
    public static Block bigint_percentiles(@SqlType(StandardTypes.VARBINARY) Slice serializedDigest,
                                           @SqlType("array(double)") Block percentilesArrayBlock) {
        List<Double> percentiles = getPercentileListFromBlock(percentilesArrayBlock);
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, percentiles.size());
        for (Long x : new QuantileDigest(serializedDigest).getQuantiles(percentiles)) {
            BIGINT.writeLong(blockBuilder, x);
        }
        return blockBuilder.build();
    }

    @ScalarFunction
    @SqlType("array(double)")
    public static Block percentiles(@SqlType(StandardTypes.VARBINARY) Slice serializedDigest,
                                    @SqlType("array(double)") Block percentilesArrayBlock) {
        List<Double> percentiles = getPercentileListFromBlock(percentilesArrayBlock);
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, percentiles.size());
        for (Long x : new QuantileDigest(serializedDigest).getQuantiles(percentiles)) {
            DOUBLE.writeDouble(blockBuilder, sortableLongToDouble(x));
        }
        return blockBuilder.build();
    }

    @ScalarFunction
    @SqlType("array(real)")
    public static Block real_percentiles(@SqlType(StandardTypes.VARBINARY) Slice serializedDigest,
                                         @SqlType("array(double)") Block percentilesArrayBlock) {
        List<Double> percentiles = getPercentileListFromBlock(percentilesArrayBlock);
        BlockBuilder blockBuilder = REAL.createBlockBuilder(null, percentiles.size());
        for (Long x : new QuantileDigest(serializedDigest).getQuantiles(percentiles)) {
            REAL.writeLong(blockBuilder, floatToRawIntBits(sortableIntToFloat(Math.toIntExact(x))));
        }
        return blockBuilder.build();
    }

    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double digest_confidence(@SqlType(StandardTypes.VARBINARY) Slice serializedDigest) {
        return new QuantileDigest(serializedDigest).getConfidenceFactor();
    }

    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double digest_count(@SqlType(StandardTypes.VARBINARY) Slice serializedDigest) {
        return new QuantileDigest(serializedDigest).getCount();
    }

    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double digest_alpha(@SqlType(StandardTypes.VARBINARY) Slice serializedDigest) {
        return new QuantileDigest(serializedDigest).getAlpha();
    }

    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double digest_error(@SqlType(StandardTypes.VARBINARY) Slice serializedDigest) {
        return new QuantileDigest(serializedDigest).getMaxError();
    }

    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double digest_min(@SqlType(StandardTypes.VARBINARY) Slice serializedDigest) {
        return sortableLongToDouble(new QuantileDigest(serializedDigest).getMin());
    }

    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double digest_max(@SqlType(StandardTypes.VARBINARY) Slice serializedDigest) {
        return sortableLongToDouble(new QuantileDigest(serializedDigest).getMax());
    }

    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double digest_lower(@SqlType(StandardTypes.VARBINARY) Slice serializedDigest,
                                      @SqlType(StandardTypes.DOUBLE) double percentile) {
        return sortableLongToDouble(new QuantileDigest(serializedDigest).getQuantileLowerBound(percentile));
    }

    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double digest_upper(@SqlType(StandardTypes.VARBINARY) Slice serializedDigest,
                                      @SqlType(StandardTypes.DOUBLE) double percentile) {
        return sortableLongToDouble(new QuantileDigest(serializedDigest).getQuantileUpperBound(percentile));
    }

    @ScalarFunction
    @SqlType("array(double)")
    public static Block digest_histogram(@SqlType(StandardTypes.VARBINARY) Slice serializedDigest,
                                 @SqlType("array(bigint)") Block longArrayBlock) {
        List<Long> upperBounds = getLongArrayFromBlock(longArrayBlock);
        List<QuantileDigest.Bucket> buckets = new QuantileDigest(serializedDigest).getHistogram(upperBounds);
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, 2 * buckets.size());
        for (QuantileDigest.Bucket x : buckets) {
            DOUBLE.writeDouble(blockBuilder, x.getCount());
            DOUBLE.writeDouble(blockBuilder, x.getMean());
        }
        return blockBuilder.build();
    }

    private static List<Double> getPercentileListFromBlock(Block percentilesArrayBlock) {
        ImmutableList.Builder<Double> percentilesListBuilder = ImmutableList.builder();
        for (int i = 0; i < percentilesArrayBlock.getPositionCount(); i++) {
            checkCondition(!percentilesArrayBlock.isNull(i), INVALID_FUNCTION_ARGUMENT, "Percentile cannot be null");
            double percentile = DOUBLE.getDouble(percentilesArrayBlock, i);
            checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
            percentilesListBuilder.add(percentile);
        }
        return percentilesListBuilder.build();
    }

    private static List<Long> getLongArrayFromBlock(Block arrayBlock) {
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            checkCondition(!arrayBlock.isNull(i), INVALID_FUNCTION_ARGUMENT, "value cannot be null");
            long value = BIGINT.getLong(arrayBlock, i);
            builder.add(value);
        }
        return builder.build();
    }

    private static double sortableLongToDouble(long value) {
        value = value ^ (value >> 63) & Long.MAX_VALUE;
        return Double.longBitsToDouble(value);
    }

    private static float sortableIntToFloat(int value) {
        value = value ^ (value >> 31) & Integer.MAX_VALUE;
        return Float.intBitsToFloat(value);
    }
}
