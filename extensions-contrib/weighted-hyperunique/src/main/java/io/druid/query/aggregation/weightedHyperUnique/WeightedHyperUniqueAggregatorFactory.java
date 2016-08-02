package io.druid.query.aggregation.weightedHyperUnique;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.metamx.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import com.metamx.common.logger.Logger;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static java.lang.Double.*;

/**
 * Created by sajo on 2/8/16.
 */
@JsonTypeName("weightedHyperUnique")
public class WeightedHyperUniqueAggregatorFactory extends AggregatorFactory{
    private static final Logger log = new Logger(WeightedHyperUniqueAggregatorFactory.class);
    private static final byte CACHE_TYPE_ID = 'Z';
    private final String name;
    private final String fieldName;

    @JsonCreator
    public WeightedHyperUniqueAggregatorFactory(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") String fieldName
    ){
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(fieldName);
        log.debug(name);
        log.debug(fieldName);

        this.name = name;
        this.fieldName = fieldName;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory) {
        return new WeightedHyperUniqueAggregator(name, metricFactory.makeFloatColumnSelector(fieldName));
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
        return new WeightedHyperUniqueBufferedAggregator(metricFactory.makeFloatColumnSelector(fieldName));
    }

    @Override
    public Comparator getComparator() {
        return WeightedHyperUniqueAggregator.COMPARATOR;
    }

    /**
     * A method that knows how to combine the outputs of the getIntermediate() method from the Aggregators
     * produced via factorize().  Note, even though this is called combine, this method's contract *does*
     * allow for mutation of the input objects.  Thus, any use of lhs or rhs after calling this method is
     * highly discouraged.
     *
     * @param lhs The left hand side of the combine
     * @param rhs The right hand side of the combine
     * @return an object representing the combination of lhs and rhs, this can be a new object or a mutation of the inputs
     */
    @Override
    public Object combine(Object lhs, Object rhs) {
        return WeightedHyperUniqueAggregator.combine(lhs, rhs);
    }

    /**
     * Returns an AggregatorFactory that can be used to combine the output of aggregators from this factory.  This
     * generally amounts to simply creating a new factory that is the same as the current except with its input
     * column renamed to the same as the output column.
     *
     * @return a new Factory that can be used for operations on top of data output from the current factory.
     */
    @Override
    public AggregatorFactory getCombiningFactory() {
        return new WeightedHyperUniqueAggregatorFactory(name, name);
    }

    /**
     * Gets a list of all columns that this AggregatorFactory will scan
     *
     * @return AggregatorFactories for the columns to scan of the parent AggregatorFactory
     */
    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        return Arrays.<AggregatorFactory>asList(new WeightedHyperUniqueAggregatorFactory(fieldName, fieldName));
    }

    /**
     * A method that knows how to "deserialize" the object from whatever form it might have been put into
     * in order to transfer via JSON.
     *
     * @param object the object to deserialize
     * @return the deserialized object
     */
    @Override
    public Object deserialize(Object object) {
        if (object instanceof String){
            return parseDouble((String) object);
        }
        return object;
    }

    /**
     * "Finalizes" the computation of an object.  Primarily useful for complex types that have a different mergeable
     * intermediate format than their final resultant output.
     *
     * @param object the object to be finalized
     * @return the finalized value that should be returned for the initial query
     */
    @Override
    public Object finalizeComputation(Object object) {
        return object;
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
     * Returns the maximum size that this aggregator will require in bytes for intermediate storage of results.
     *
     * @return the maximum number of bytes that an aggregator of this type will require for intermediate result storage.
     */
    @Override
    public int getMaxIntermediateSize() {
        return Double.SIZE;
    }

    /**
     * Returns the starting value for a corresponding aggregator. For example, 0 for sums, - Infinity for max, an empty mogrifier
     *
     * @return the starting value for a corresponding aggregator.
     */
    @Override
    public Object getAggregatorStartValue() {
        return 0;
    }
}
