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
import io.airlift.slice.Slice;
import io.airlift.stats.QuantileDigest;
import io.panyu.skydrill.operator.aggregation.state.QuantileDigestState;

@AggregationFunction("merge_digest")
public final class MergeQuantileDigestAggregation {
    private static final AccumulatorStateSerializer<QuantileDigestState> serializer = StateCompiler.generateStateSerializer(QuantileDigestState.class);

    private MergeQuantileDigestAggregation() {}

    @InputFunction
    public static void input(@AggregationState QuantileDigestState state, @SqlType(StandardTypes.VARBINARY) Slice value) {
        QuantileDigest input = new QuantileDigest(value);
        merge(state, input);
    }

    @CombineFunction
    public static void combine(@AggregationState QuantileDigestState state, @AggregationState QuantileDigestState otherState) {
        merge(state, otherState.getDigest());
    }

    private static void merge(@AggregationState QuantileDigestState state, QuantileDigest input) {
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
    public static void output(@AggregationState QuantileDigestState state, BlockBuilder out) {
        serializer.serialize(state, out);
    }
}
