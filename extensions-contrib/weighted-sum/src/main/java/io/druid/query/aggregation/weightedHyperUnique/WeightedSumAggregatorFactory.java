package io.druid.query.aggregation.weightedHyperUnique;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.metamx.common.IAE;
import com.metamx.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import com.metamx.common.logger.Logger;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Created by sajo on 2/8/16.
 */
@JsonTypeName("weightedHyperUnique")
public class WeightedSumAggregatorFactory extends AggregatorFactory{
    private static final Logger log = new Logger(WeightedSumAggregatorFactory.class);
    private static final byte CACHE_TYPE_ID = 'Z';
    private final String name;
    private final String fieldName;

    @JsonCreator
    public WeightedSumAggregatorFactory(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") String fieldName
    ){
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(fieldName);

        this.name = name;
        this.fieldName = fieldName;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory) {
        final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

        if (selector == null){
            throw new IAE(
                    "field selector for metric[%s] was null",
                    fieldName
            );
        }

        final Class cls = selector.classOfObject();
        if(cls.equals(Object.class) || WeightedSumUnique.class.isAssignableFrom(cls)){
            return new WeightedSumAggregator(name, selector);
        }
        else if(cls.equals(Float.TYPE) || Float.TYPE.isAssignableFrom(cls)){
            throw new IAE(
                    "Aggregation called for class %s (not supported). Aggregator: %s, Fieldname: %s",
                    cls,
                    name,
                    fieldName
            );
        }
        else{
            throw new IAE(
                    "Incompatible type for metric[%s], expected a Float, got a %s",
                    fieldName,
                    cls
            );
        }
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
        final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

        if(selector == null){
            throw new IAE(
                    "field selector for metric[%s] was null in factorizeBuffered",
                    fieldName
            );
        }

        final Class cls = selector.classOfObject();

        if(cls.equals(WeightedSumUnique.class) || WeightedSumUnique.class.isAssignableFrom(cls)){
            throw new IAE(
                    "preaggregated WeightedSumUnique is not supported yet"
            );
            //TODO: Should return a combining Aggregator here
        }
        else if(cls.equals(Float.TYPE) || Float.TYPE.isAssignableFrom(cls)){
            final FloatColumnSelector rawSelector = metricFactory.makeFloatColumnSelector(fieldName);
            return new WeightedSumBufferedAggregator(rawSelector, 1);
        }
        else{
            throw new IAE(
                    "Incompatible type for metric[%s], expected a Float, got a %s",
                    fieldName,
                    cls
            );
        }
    }

    @Override
    public Comparator getComparator() {
        return WeightedSumAggregator.COMPARATOR;
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
        return WeightedSumAggregator.combine(lhs, rhs);
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
        log.error("get combining factory");
        return new WeightedSumAggregatorFactory(name, name);
    }

    @Override
    public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
      {
        if (other.getName().equals(this.getName()) && other instanceof WeightedSumAggregatorFactory) {
          WeightedSumAggregatorFactory castedOther = (WeightedSumAggregatorFactory) other;

          return new WeightedSumAggregatorFactory(
              name,
              name
          );
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
        return Arrays.<AggregatorFactory>asList(new WeightedSumAggregatorFactory(fieldName, fieldName));
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
       if(object instanceof byte[]) {
           final WeightedSumUnique w = WeightedSumUnique.fromBytes((byte[]) object);
           return w;
       }
       else {
           throw new IAE(
                   "Don't know how to deserialze %s to WeightedSumUnique",
                   object.getClass()
           );
       }
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
        return ((WeightedSumUnique)(object)).getValue();
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

    @Override
    public boolean equals(Object o){
        if (this == o){
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }

        WeightedSumAggregatorFactory that = (WeightedSumAggregatorFactory) o;

        if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode(){
        int result = name != null ? name.hashCode():0;
        result = 31*result + fieldName != null ? fieldName.hashCode():0;
        return result;
    }

    @Override
    public String toString(){
        return "WeightedSumAggregatorFactory{"+
                "name="+name+
                ", fieldname="+fieldName+
                "}";
    }
}
