package io.panyu.skydrill.operator.aggregation;

import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.stats.QuantileDigest;
import io.panyu.skydrill.operator.aggregation.state.QuantileDigestState;

import static java.lang.Float.intBitsToFloat;

@AggregationFunction("make_digest")
public final class QuantileDigestAggregation
{
    private static final AccumulatorStateSerializer<QuantileDigestState> serializer = StateCompiler.generateStateSerializer(QuantileDigestState.class);

    private QuantileDigestAggregation() {}

    @InputFunction
    public static void input(@AggregationState QuantileDigestState state,
                             @SqlType(StandardTypes.BIGINT) long value) {
        QuantileDigest digest = state.getDigest();

        if (digest == null) {
            digest = new QuantileDigest(0.01);
            state.setDigest(digest);
            state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
        }

        state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
        digest.add(value);
        state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
    }

    @InputFunction
    public static void input(@AggregationState QuantileDigestState state,
                             @SqlType(StandardTypes.DOUBLE) double value) {
        input(state, doubleToSortableLong(value));
    }

    @InputFunction
    public static void input(@AggregationState QuantileDigestState state,
                             @SqlType(StandardTypes.REAL) int value) {
        input(state, floatToSortableInt(intBitsToFloat(value)));
    }

    @CombineFunction
    public static void combine(@AggregationState QuantileDigestState state,
                               QuantileDigestState otherState) {
        QuantileDigest input = otherState.getDigest();

        QuantileDigest previous = state.getDigest();
        if (previous == null) {
            state.setDigest(input);
            state.addMemoryUsage(input.estimatedInMemorySizeInBytes());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySizeInBytes());
            previous.merge(input);
            state.addMemoryUsage(previous.estimatedInMemorySizeInBytes());
        }
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(@AggregationState QuantileDigestState state,
                              BlockBuilder out) {
        serializer.serialize(state, out);
    }

    private static long doubleToSortableLong(double value) {
        long bits = Double.doubleToLongBits(value);
        return bits ^ (bits >> 63) & Long.MAX_VALUE;
    }

    private static int floatToSortableInt(float value) {
        int bits = Float.floatToIntBits(value);
        return bits ^ (bits >> 31) & Integer.MAX_VALUE;
    }
}
