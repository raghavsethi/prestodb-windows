package io.panyu.skydrill.operator.aggregation.state;

import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;
import io.airlift.stats.QuantileDigest;

@AccumulatorStateMetadata(stateSerializerClass = QuantileDigestStateSerializer.class, stateFactoryClass = QuantileDigestStateFactory.class)
public interface QuantileDigestState
        extends AccumulatorState
{
    QuantileDigest getDigest();

    void setDigest(QuantileDigest digest);

    void addMemoryUsage(int value);
}
