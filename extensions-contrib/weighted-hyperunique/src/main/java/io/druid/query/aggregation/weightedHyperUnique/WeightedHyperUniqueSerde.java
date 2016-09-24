package io.druid.query.aggregation.weightedHyperUnique;

import com.google.common.collect.Ordering;
import com.google.common.hash.HashFunction;

import com.metamx.common.StringUtils;

import java.nio.ByteBuffer;
import java.util.List;

import io.druid.data.input.InputRow;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

/**
 * Created by sajo on 2/8/16.
 */

public class WeightedHyperUniqueSerde extends ComplexMetricSerde {

    private static Ordering<HyperLogLogCollector> comparator = new Ordering<HyperLogLogCollector>() {
        @Override
        public int compare(
                HyperLogLogCollector arg1, HyperLogLogCollector arg2
        ) {
            return arg1.toByteBuffer().compareTo(arg2.toByteBuffer());
        }
    }.nullsFirst();

    private final HashFunction hashFn;

    public WeightedHyperUniqueSerde(HashFunction hashFn) {
        this.hashFn = hashFn;
    }

    @Override
    public String getTypeName() {
        return "weightedHyperUnique";
    }


    /**
     * Deserializes a ByteBuffer and adds it to the ColumnBuilder.  This method allows for the
     * ComplexMetricSerde to implement it's own versioning scheme to allow for changes of binary
     * format in a forward-compatible manner.
     *
     * @param byteBuffer    the buffer to deserialize
     * @param columnBuilder ColumnBuilder to add the column to
     */
    @Override
    public void deserializeColumn(ByteBuffer byteBuffer, ColumnBuilder columnBuilder) {
        final GenericIndexed column = GenericIndexed.read(byteBuffer, getObjectStrategy());
        columnBuilder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), column));

    }

    @Override
    public ComplexMetricExtractor getExtractor() {
        return new ComplexMetricExtractor() {
            @Override
            public Class<HyperLogLogCollector> extractedClass() {
                return HyperLogLogCollector.class;
            }

            @Override
            public HyperLogLogCollector extractValue(InputRow inputRow, String metricName) {
                Object rawValue = inputRow.getRaw(metricName);

                if (rawValue instanceof HyperLogLogCollector) {
                    return (HyperLogLogCollector) rawValue;
                } else {
                    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

                    List<String> dimValues = inputRow.getDimension(metricName);
                    if (dimValues == null) {
                        return collector;
                    }

                    for (String dimensionValue : dimValues) {
                        WeightedUserId json = WeightedUserId.fromJson(dimensionValue);
                        int weight = json.getWeight();
                        String userId = json.getId();
                        for (int i = 0; i < weight; i++) {
                            String modifiedDimension = userId + ":" + i;
                            collector.add(
                                    hashFn.hashBytes(StringUtils.toUtf8(modifiedDimension)).asBytes()
                            );
                        }
                    }
                    return collector;
                }
            }
        };
    }

    /**
     * This is deprecated because its usage is going to be removed from the code. <p> It was
     * introduced before deserializeColumn() existed.  This method creates the assumption that Druid
     * knows how to interpret the actual column representation of the data, but I would much prefer
     * that the ComplexMetricSerde objects be in charge of creating and interpreting the whole
     * column, which is what deserializeColumn lets them do.
     *
     * @return an ObjectStrategy as used by GenericIndexed
     */
    @Override
    public ObjectStrategy getObjectStrategy() {
        return new ObjectStrategy<HyperLogLogCollector>() {
            @Override
            public Class<? extends HyperLogLogCollector> getClazz() {
                return HyperLogLogCollector.class;
            }

            @Override
            public HyperLogLogCollector fromByteBuffer(ByteBuffer buffer, int numBytes) {
                final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
                readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
                return HyperLogLogCollector.makeCollector(readOnlyBuffer);
            }

            @Override
            public byte[] toBytes(HyperLogLogCollector collector) {
                if (collector == null) {
                    return new byte[]{};
                }
                ByteBuffer val = collector.toByteBuffer();
                byte[] retVal = new byte[val.remaining()];
                val.asReadOnlyBuffer().get(retVal);
                return retVal;
            }

            @Override
            public int compare(HyperLogLogCollector o1, HyperLogLogCollector o2) {
                return comparator.compare(o1, o2);
            }
        };
    }
}
