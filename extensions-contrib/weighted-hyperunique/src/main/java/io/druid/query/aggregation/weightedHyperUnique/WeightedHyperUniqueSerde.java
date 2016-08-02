package io.druid.query.aggregation.weightedHyperUnique;

import com.google.common.collect.Ordering;
import io.druid.data.input.InputRow;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Created by sajo on 2/8/16.
 */
public class WeightedHyperUniqueSerde extends ComplexMetricSerde {

    private static Ordering<Double> comparator = new Ordering<Double>() {
        @Override
        public int compare(@Nullable Double aDouble, @Nullable Double t1) {
            return WeightedHyperUniqueAggregator.COMPARATOR.compare(aDouble, t1);
        }
    };
    @Override
    public String getTypeName() {
        return "weightedHyperUnique";
    }


    /**
     * Deserializes a ByteBuffer and adds it to the ColumnBuilder.  This method allows for the ComplexMetricSerde
     * to implement it's own versioning scheme to allow for changes of binary format in a forward-compatible manner.
     *
     * @param buffer  the buffer to deserialize
     * @param builder ColumnBuilder to add the column to
     */
    @Override
    public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder) {
        final GenericIndexed column = GenericIndexed.read(buffer, getObjectStrategy());
        builder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), column));

    }

    @Override
    public ComplexMetricExtractor getExtractor(){
        return new ComplexMetricExtractor() {
            @Override
            public Class<Double> extractedClass() {
                return Double.class;
            }

            @Override
            public Object extractValue(InputRow inputRow, String metricName) {
                double data = inputRow.getFloatMetric(metricName);
                return data;
            }
        };
    }

    /**
     * This is deprecated because its usage is going to be removed from the code.
     * <p>
     * It was introduced before deserializeColumn() existed.  This method creates the assumption that Druid knows
     * how to interpret the actual column representation of the data, but I would much prefer that the ComplexMetricSerde
     * objects be in charge of creating and interpreting the whole column, which is what deserializeColumn lets
     * them do.
     *
     * @return an ObjectStrategy as used by GenericIndexed
     */
    @Override
    public ObjectStrategy getObjectStrategy() {
        return new ObjectStrategy<Double>() {
            @Override
            public Class getClazz() {
                return Double.class;
            }

            @Override
            public Double fromByteBuffer(ByteBuffer buffer, int numBytes) {
                final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
                readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
                return readOnlyBuffer.getDouble();
            }

            @Override
            public byte[] toBytes(Double val) {
                byte[] bytes = new byte[Double.SIZE];
                ByteBuffer.wrap(bytes).putDouble(val);
                return bytes;
            }

            @Override
            public int compare(Double o1, Double o2) {
                return comparator.compare(o1, o2);
            }
        };
    }
}
