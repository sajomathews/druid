package io.druid.query.aggregation.weightedHyperUnique;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.FloatColumn;

import java.nio.ByteBuffer;

/**
 * Created by sajo on 2/8/16.
 */
public class WeightedHyperUniqueBufferedAggregator implements BufferAggregator {

    private final FloatColumnSelector selector;
    private final Integer weight;

    public WeightedHyperUniqueBufferedAggregator(FloatColumnSelector objectColumnSelector, Integer weight) {
        this.selector = objectColumnSelector;
        this.weight = weight;
    }

    @Override
    public void init(ByteBuffer buf, int position) {
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position);

        WeightedHyperUnique w = new WeightedHyperUnique(0);
        w.toByteBuf(mutationBuffer);
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

        WeightedHyperUnique current = WeightedHyperUnique.fromByteBuf(mutationBuffer.asReadOnlyBuffer());
        current.offer(weight * selector.get());

        mutationBuffer.position(position);
        current.toByteBuf(mutationBuffer);
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
        ByteBuffer mutationBuffer = buf.asReadOnlyBuffer();
        mutationBuffer.position(position);

        return WeightedHyperUnique.fromByteBuf(mutationBuffer);
    }

    @Override
    public float getFloat(ByteBuffer buf, int position) {
        ByteBuffer mutationBuffer = buf.asReadOnlyBuffer();
        mutationBuffer.position(position);

        return WeightedHyperUnique.fromByteBuf(mutationBuffer).getValue();
    }

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
