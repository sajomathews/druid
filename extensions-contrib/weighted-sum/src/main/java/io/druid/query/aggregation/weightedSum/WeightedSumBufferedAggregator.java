package io.druid.query.aggregation.weightedSum;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

/**
 * Created by sajo on 2/8/16.
 */
public class WeightedSumBufferedAggregator implements BufferAggregator {

    private final ObjectColumnSelector<WeightedSum> selector;

    public WeightedSumBufferedAggregator(ObjectColumnSelector<WeightedSum> objectColumnSelector) {
        this.selector = objectColumnSelector;
    }

    @Override
    public void init(ByteBuffer buf, int position) {
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position);

        WeightedSum w = new WeightedSum(0);
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

        WeightedSum current = WeightedSum.fromByteBuf(mutationBuffer.asReadOnlyBuffer());
        current.fold((WeightedSum) selector.get());

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

        return WeightedSum.fromByteBuf(mutationBuffer);
    }

    @Override
    public float getFloat(ByteBuffer buf, int position) {
        ByteBuffer mutationBuffer = buf.asReadOnlyBuffer();
        mutationBuffer.position(position);

        return WeightedSum.fromByteBuf(mutationBuffer).getValue();
    }

    @Override
    public long getLong(ByteBuffer buf, int position) {
        throw new UnsupportedOperationException("WeightedSumAggregator does not support getLong()");
        //TODO: Should support getLong()
    }

    /**
     * Release any resources used by the aggregator
     */
    @Override
    public void close() {

    }
}
