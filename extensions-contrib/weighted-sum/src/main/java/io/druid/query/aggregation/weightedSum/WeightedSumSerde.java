package io.druid.query.aggregation.weightedSum;

import com.google.common.collect.Ordering;
import com.metamx.common.IAE;
import com.metamx.common.logger.Logger;
import io.druid.data.input.InputRow;
import io.druid.segment.column.ColumnBuilder;
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

public class WeightedSumSerde extends ComplexMetricSerde {

    private static Ordering<WeightedSum> comparator = new Ordering<WeightedSum>() {
        @Override
        public int compare(@Nullable WeightedSum w1, @Nullable WeightedSum t1) {
            return WeightedSumAggregator.COMPARATOR.compare(w1, t1);
        }
    };
    @Override
    public String getTypeName() {
        return "weightedSum";
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

            private final Logger log = new Logger("io.druid.initialization.Initialization");
            @Override
            public Class<WeightedSum> extractedClass() {
                return WeightedSum.class;
            }

            @Override
            public Object extractValue(InputRow inputRow, String metricName) {
                Object raw_value = inputRow.getRaw(metricName);

                log.error(String.format("input row outside is : %s",inputRow));

                if (raw_value instanceof WeightedSum){
                    log.error(String.format("logged raw value is weighted sum, Metric: %s, Raw Value: %s, Class: %s", metricName, raw_value, raw_value.getClass().getSimpleName()));
                    log.error(String.format("input row is : %s",inputRow));
                    return (WeightedSum) raw_value;
                }
                else {
                    WeightedSum sum = new WeightedSum(0);
                    List<String> dimValues = inputRow.getDimension(metricName);
                    if (dimValues == null){
                        throw new IAE("raw value is null");
                        //return sum;
                    }
                    else {
                        log.error("Getting raw values");
                        for (String value: dimValues){
                            WeightedDuration duration = WeightedDuration.fromJson(value);
                            sum.offer((float) (duration.getWeight() * duration.getDuration()));
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
        return new ObjectStrategy<WeightedSum>() {
            @Override
            public Class<? extends WeightedSum> getClazz() {
                return WeightedSum.class;
            }

            @Override
            public WeightedSum fromByteBuffer(ByteBuffer buffer, int numBytes) {
                final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
                readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
                byte[] b = new byte[readOnlyBuffer.remaining()];
                readOnlyBuffer.get(b);
                return WeightedSum.fromBytes(b);
            }

            @Override
            public byte[] toBytes(WeightedSum val) {
                return val.toBytes();
            }

            @Override
            public int compare(WeightedSum w1, WeightedSum w2) {
                return comparator.compare(w1, w2);
            }
        };
    }
}
