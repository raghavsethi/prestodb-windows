package io.panyu.skydrill.operator.aggregation.state;

import com.facebook.presto.array.DoubleBigArray;
import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import io.airlift.stats.QuantileDigest;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class QuantileDigestStateFactory
        implements AccumulatorStateFactory<QuantileDigestState>
{
    @Override
    public QuantileDigestState createSingleState() {
        return new SingleDigestState();
    }

    @Override
    public Class<? extends QuantileDigestState> getSingleStateClass() {
        return SingleDigestState.class;
    }

    @Override
    public QuantileDigestState createGroupedState() {
        return new GroupedDigestState();
    }

    @Override
    public Class<? extends QuantileDigestState> getGroupedStateClass() {
        return GroupedDigestState.class;
    }

    public static class GroupedDigestState
            extends AbstractGroupedAccumulatorState
            implements QuantileDigestState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(QuantileDigestStateFactory.GroupedDigestState.class).instanceSize();
        private final ObjectBigArray<QuantileDigest> digests = new ObjectBigArray<>();
        private final DoubleBigArray percentiles = new DoubleBigArray();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
            percentiles.ensureCapacity(size);
        }

        @Override
        public QuantileDigest getDigest()
        {
            return digests.get(getGroupId());
        }

        @Override
        public void setDigest(QuantileDigest digest)
        {
            requireNonNull(digest, "value is null");
            digests.set(getGroupId(), digest);
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + digests.sizeOf() + percentiles.sizeOf();
        }
    }

    public static class SingleDigestState
            implements QuantileDigestState
    {
        public static final int INSTANCE_SIZE = ClassLayout.parseClass(QuantileDigestStateFactory.SingleDigestState.class).instanceSize();
        private QuantileDigest digest;
        private double percentile;

        @Override
        public QuantileDigest getDigest()
        {
            return digest;
        }

        @Override
        public void setDigest(QuantileDigest digest)
        {
            this.digest = digest;
        }

        @Override
        public void addMemoryUsage(int value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (digest != null) {
                estimatedSize += digest.estimatedInMemorySizeInBytes();
            }
            return estimatedSize;
        }
    }
}
