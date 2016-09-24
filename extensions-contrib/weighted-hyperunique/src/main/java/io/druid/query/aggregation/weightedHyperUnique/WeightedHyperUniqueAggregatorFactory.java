package io.druid.query.aggregation.weightedHyperUnique;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.metamx.common.IAE;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;

import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.ObjectColumnSelector;

/**
 * Created by sajo on 2/8/16.
 */
@JsonTypeName("weightedHyperUnique")
public class WeightedHyperUniqueAggregatorFactory extends AggregatorFactory {
    private static final Logger log = new Logger(WeightedHyperUniqueAggregatorFactory.class);
    private static final byte CACHE_TYPE_ID = 'Z';
    private final String name;
    private final String fieldName;

    public static Object estimateCardinality(Object object) {
        if (object == null) {
            return 0;
        }
        return ((HyperLogLogCollector) object).estimateCardinality();
    }


    @JsonCreator
    public WeightedHyperUniqueAggregatorFactory(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") String fieldName
    ) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(fieldName);

        this.name = name;
        this.fieldName = fieldName;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory) {
        final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

        if (selector == null) {
            throw new IAE(
                    "field selector for metric[%s] was null",
                    fieldName
            );
        }

        final Class classOfObject = selector.classOfObject();
        if (classOfObject.equals(Object.class) || HyperLogLogCollector.class.isAssignableFrom(classOfObject)) {
            return new WeightedHyperUniqueAggregator(name, selector);
        } else {
            throw new IAE(
                    "Incompatible type for metric[%s], expected a HyperUnique, got a %s",
                    fieldName,
                    classOfObject
            );
        }
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
        final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

        if (selector == null) {
            throw new IAE(
                    "field selector for metric[%s] was null in factorizeBuffered",
                    fieldName
            );
        }

        final Class classOfObject = selector.classOfObject();

        if (classOfObject.equals(Object.class) || HyperLogLogCollector.class.isAssignableFrom
                (classOfObject)) {
            return new WeightedHyperUniqueBufferedAggregator(selector);
        } else {
            throw new IAE(
                    "Incompatible type for metric[%s], expected a HyperLogLogCollector, got a %s",
                    fieldName,
                    classOfObject
            );
        }
    }

    @Override
    public Comparator getComparator() {
        return Ordering.<HyperLogLogCollector>natural().nullsFirst();
    }

    /**
     * A method that knows how to combine the outputs of the getIntermediate() method from the
     * Aggregators produced via factorize().  Note, even though this is called combine, this
     * method's contract *does* allow for mutation of the input objects.  Thus, any use of lhs or
     * rhs after calling this method is highly discouraged.
     *
     * @param lhs The left hand side of the combine
     * @param rhs The right hand side of the combine
     * @return an object representing the combination of lhs and rhs, this can be a new object or a
     * mutation of the inputs
     */
    @Override
    public Object combine(Object lhs, Object rhs) {
        if (rhs == null) {
            return lhs;
        }
        if (lhs == null) {
            return rhs;
        }
        return ((HyperLogLogCollector) lhs).fold((HyperLogLogCollector) rhs);
    }

    /**
     * Returns an AggregatorFactory that can be used to combine the output of aggregators from this
     * factory.  This generally amounts to simply creating a new factory that is the same as the
     * current except with its input column renamed to the same as the output column.
     *
     * @return a new Factory that can be used for operations on top of data output from the current
     * factory.
     */
    @Override
    public AggregatorFactory getCombiningFactory() {
        return new WeightedHyperUniqueAggregatorFactory(name, name);
    }

    @Override
    public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException {
        if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
            return getCombiningFactory();
        } else {
            throw new AggregatorFactoryNotMergeableException(this, other);
        }
    }

    /**
     * Gets a list of all columns that this AggregatorFactory will scan
     *
     * @return AggregatorFactories for the columns to scan of the parent AggregatorFactory
     */
    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        //TODO: Figure out when this is called
        return Arrays.<AggregatorFactory>asList(new WeightedHyperUniqueAggregatorFactory(fieldName, fieldName));
    }

    /**
     * A method that knows how to "deserialize" the object from whatever form it might have been put
     * into in order to transfer via JSON.
     *
     * @param object the object to deserialize
     * @return the deserialized object
     */
    @Override
    public Object deserialize(Object object) {
        if (object instanceof byte[]) {
            return HyperLogLogCollector.makeCollector(ByteBuffer.wrap((byte[]) object));
        } else if (object instanceof ByteBuffer) {
            return HyperLogLogCollector.makeCollector((ByteBuffer) object);
        } else if (object instanceof String) {
            return HyperLogLogCollector.makeCollector(
                    ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)))
            );
        }
        return object;
    }

    /**
     * "Finalizes" the computation of an object.  Primarily useful for complex types that have a
     * different mergeable intermediate format than their final resultant output.
     *
     * @param object the object to be finalized
     * @return the finalized value that should be returned for the initial query
     */
    @Override
    public Object finalizeComputation(Object object) {
        return estimateCardinality(object);
    }

    @Override
    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public List<String> requiredFields() {
        return Arrays.asList(fieldName);
    }

    @Override
    public byte[] getCacheKey() {
        byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
        return ByteBuffer.allocate(fieldNameBytes.length + 1).put(CACHE_TYPE_ID).put(fieldNameBytes).array();
    }

    @Override
    public String getTypeName() {
        return "weightedHyperUnique";
    }

    /**
     * Returns the maximum size that this aggregator will require in bytes for intermediate storage
     * of results.
     *
     * @return the maximum number of bytes that an aggregator of this type will require for
     * intermediate result storage.
     */
    @Override
    public int getMaxIntermediateSize() {
        return HyperLogLogCollector.getLatestNumBytesForDenseStorage();
    }

    /**
     * Returns the starting value for a corresponding aggregator. For example, 0 for sums, -
     * Infinity for max, an empty mogrifier
     *
     * @return the starting value for a corresponding aggregator.
     */
    @Override
    public Object getAggregatorStartValue() {
        return HyperLogLogCollector.makeLatestCollector();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WeightedHyperUniqueAggregatorFactory that = (WeightedHyperUniqueAggregatorFactory) o;

        if (!fieldName.equals(that.fieldName)) return false;
        if (!name.equals(that.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + fieldName != null ? fieldName.hashCode() : 0;
        return result;
    }

    @Override
    public String toString() {
        return "WeightedHyperUniqueAggregatorFactory{" +
                "name=" + name +
                ", fieldname=" + fieldName +
                "}";
    }
}
