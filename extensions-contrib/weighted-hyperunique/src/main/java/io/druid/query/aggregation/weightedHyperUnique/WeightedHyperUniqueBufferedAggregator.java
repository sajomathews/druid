package io.druid.query.aggregation.weightedHyperUnique;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.FloatColumnSelector;

import java.nio.ByteBuffer;

/**
 * Created by sajo on 2/8/16.
 */
public class WeightedHyperUniqueBufferedAggregator implements BufferAggregator {

    private final FloatColumnSelector selector;

    public WeightedHyperUniqueBufferedAggregator(FloatColumnSelector floatColumnSelector) {
        this.selector = floatColumnSelector;
    }

    /**
     * Initializes the buffer location
     * <p>
     * Implementations of this method must initialize the byte buffer at the given position
     * <p>
     * <b>Implementations must not change the position, limit or mark of the given buffer</b>
     * <p>
     * This method must not exceed the number of bytes returned by {@link AggregatorFactory#getMaxIntermediateSize()}
     * in the corresponding {@link AggregatorFactory}
     *
     * @param buf      byte buffer to initialize
     * @param position offset within the byte buffer for initialization
     */
    @Override
    public void init(ByteBuffer buf, int position) {
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position);

        mutationBuffer.putDouble(0d);
    }

    /**
     * Aggregates metric values into the given aggregate byte representation
     * <p>
     * Implementations of this method must read in the aggregate value from the buffer at the given position,
     * aggregate the next element of data and write the updated aggregate value back into the buffer.
     * <p>
     * <b>Implementations must not change the position, limit or mark of the given buffer</b>
     *
     * @param buf      byte buffer storing the byte array representation of the aggregate
     * @param position offset within the byte buffer at which the current aggregate value is stored
     */
    @Override
    public void aggregate(ByteBuffer buf, int position) {
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position);

        double current = mutationBuffer.getDouble();

        mutationBuffer.position(position);
        mutationBuffer.putDouble(current + selector.get());
    }

    /**
     * Returns the intermediate object representation of the given aggregate.
     * <p>
     * Converts the given byte buffer representation into an intermediate aggregate Object
     * <p>
     * <b>Implementations must not change the position, limit or mark of the given buffer</b>
     *
     * @param buf      byte buffer storing the byte array representation of the aggregate
     * @param position offset within the byte buffer at which the aggregate value is stored
     * @return the Object representation of the aggregate
     */
    @Override
    public Object get(ByteBuffer buf, int position) {
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position);

        return mutationBuffer.getDouble();
    }

    /**
     * Returns the float representation of the given aggregate byte array
     * <p>
     * Converts the given byte buffer representation into the intermediate aggregate value.
     * <p>
     * <b>Implementations must not change the position, limit or mark of the given buffer</b>
     * <p>
     * Implementations are only required to support this method if they are aggregations which
     * have an {@link AggregatorFactory#getTypeName()} of "float".
     * If unimplemented, throwing an {@link UnsupportedOperationException} is common and recommended.
     *
     * @param buf      byte buffer storing the byte array representation of the aggregate
     * @param position offset within the byte buffer at which the aggregate value is stored
     * @return the float representation of the aggregate
     */
    @Override
    public float getFloat(ByteBuffer buf, int position) {
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position);

        return (float) mutationBuffer.getDouble();
    }

    /**
     * Returns the long representation of the given aggregate byte array
     * <p>
     * Converts the given byte buffer representation into the intermediate aggregate value.
     * <p>
     * <b>Implementations must not change the position, limit or mark of the given buffer</b>
     * <p>
     * Implementations are only required to support this method if they are aggregations which
     * have an {@link AggregatorFactory#getTypeName()} of "long".
     * If unimplemented, throwing an {@link UnsupportedOperationException} is common and recommended.
     *
     * @param buf      byte buffer storing the byte array representation of the aggregate
     * @param position offset within the byte buffer at which the aggregate value is stored
     * @return the long representation of the aggregate
     */
    @Override
    public long getLong(ByteBuffer buf, int position) {
        throw new UnsupportedOperationException("WeightedUniqueAggregator does not support getLong()");
    }

    /**
     * Release any resources used by the aggregator
     */
    @Override
    public void close() {

    }
}
