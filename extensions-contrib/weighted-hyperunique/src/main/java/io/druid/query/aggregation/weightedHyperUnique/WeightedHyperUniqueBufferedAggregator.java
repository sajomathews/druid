package io.druid.query.aggregation.weightedHyperUnique;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.FloatColumn;

import java.nio.ByteBuffer;

/**
 * Created by sajo on 2/8/16.
 */
public class WeightedHyperUniqueBufferedAggregator implements BufferAggregator {

    private static final byte[] EMPTY_BYTES = HyperLogLogCollector.makeEmptyVersionedByteArray();
    private final ObjectColumnSelector selector;

    public WeightedHyperUniqueBufferedAggregator(ObjectColumnSelector objectColumnSelector) {
        this.selector = objectColumnSelector;
    }

    @Override
    public void init(ByteBuffer buf, int position) {
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position);
        mutationBuffer.put(EMPTY_BYTES);
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
        HyperLogLogCollector collector = (HyperLogLogCollector) selector.get();

        if (collector == null) {
            return;
        }

        HyperLogLogCollector.makeCollector(
                (ByteBuffer) buf.duplicate().position(position).limit(
                        position
                                + HyperLogLogCollector.getLatestNumBytesForDenseStorage()
                )
        ).fold(collector);
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
        final int size = HyperLogLogCollector.getLatestNumBytesForDenseStorage();
        ByteBuffer dataCopyBuffer = ByteBuffer.allocate(size);
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position);
        mutationBuffer.limit(position + size);
        dataCopyBuffer.put(mutationBuffer);
        dataCopyBuffer.rewind();
        return HyperLogLogCollector.makeCollector(dataCopyBuffer);
    }

    @Override
    public float getFloat(ByteBuffer buf, int position) {
        throw new UnsupportedOperationException("HyperUniquesBufferAggregator does not support getFloat()");
    }

    @Override
    public long getLong(ByteBuffer buf, int position) {
        throw new UnsupportedOperationException("HyperUniquesBufferAggregator does not support getLong()");
    }

    /**
     * Release any resources used by the aggregator
     */
    @Override
    public void close() {

    }
}
