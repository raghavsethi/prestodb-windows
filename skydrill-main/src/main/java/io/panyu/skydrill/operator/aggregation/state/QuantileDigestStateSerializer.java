package io.panyu.skydrill.operator.aggregation.state;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.stats.QuantileDigest;

import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;

public class QuantileDigestStateSerializer
        implements AccumulatorStateSerializer<QuantileDigestState>
{
    @Override
    public Type getSerializedType() {
        return VARBINARY;
    }

    @Override
    public void serialize(QuantileDigestState state, BlockBuilder out) {
        if (state.getDigest() == null) {
            out.appendNull();
        }
        else {
            Slice serialized = state.getDigest().serialize();

            SliceOutput output = Slices.allocate(serialized.length()).getOutput();
            output.appendBytes(serialized);

            VARBINARY.writeSlice(out, output.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, QuantileDigestState state) {
        SliceInput input = VARBINARY.getSlice(block, index).getInput();

        QuantileDigest digest = new QuantileDigest(((BasicSliceInput) input).slice());
        state.setDigest(digest);
        state.addMemoryUsage(state.getDigest().estimatedInMemorySizeInBytes());
    }
}
