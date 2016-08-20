package io.druid.query.aggregation.weightedHyperUnique;

import com.google.common.collect.Ordering;
import com.metamx.common.logger.Logger;
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
import java.util.List;

/**
 * Created by sajo on 2/8/16.
 */
public class WeightedHyperUniqueSerde extends ComplexMetricSerde {

    private static Ordering<WeightedHyperUnique> comparator = new Ordering<WeightedHyperUnique>() {
        @Override
        public int compare(@Nullable WeightedHyperUnique w1, @Nullable WeightedHyperUnique t1) {
            return WeightedHyperUniqueAggregator.COMPARATOR.compare(w1, t1);
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

            private final Logger log = new Logger("W_SERDE");
            @Override
            public Class<WeightedHyperUnique> extractedClass() {
                return WeightedHyperUnique.class;
            }

            @Override
            public Object extractValue(InputRow inputRow, String metricName) {
                Object raw_value = inputRow.getRaw(metricName);

                if (raw_value instanceof WeightedHyperUnique){
                    return (WeightedHyperUnique) raw_value;
                }
                else {
                    WeightedHyperUnique sum = new WeightedHyperUnique(0);
                    List<String> dimValues = inputRow.getDimension(metricName);
                    if (dimValues == null){
                        return sum;
                    }
                    else {
                        for (String value: dimValues){
                                sum.offer((float) Double.parseDouble(value));
                        }
                        return sum;
                    }
                }
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
        return new ObjectStrategy<WeightedHyperUnique>() {
            @Override
            public Class<? extends WeightedHyperUnique> getClazz() {
                return WeightedHyperUnique.class;
            }

            @Override
            public WeightedHyperUnique fromByteBuffer(ByteBuffer buffer, int numBytes) {
                final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
                readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
                byte[] b = new byte[readOnlyBuffer.remaining()];
                readOnlyBuffer.get(b);
                return WeightedHyperUnique.fromBytes(b);
            }

            @Override
            public byte[] toBytes(WeightedHyperUnique val) {
                return val.toBytes();
            }

            @Override
            public int compare(WeightedHyperUnique w1, WeightedHyperUnique w2) {
                return comparator.compare(w1, w2);
            }
        };
    }
}
