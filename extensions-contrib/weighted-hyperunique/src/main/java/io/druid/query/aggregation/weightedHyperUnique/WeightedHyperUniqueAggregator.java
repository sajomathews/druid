package io.druid.query.aggregation.weightedHyperUnique;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.FloatColumnSelector;

import javax.annotation.Nullable;
import java.util.Comparator;

/**
 * Created by sajo on 2/8/16.
 */
public class WeightedHyperUniqueAggregator implements Aggregator {

    public static final Comparator COMPARATOR = new Ordering() {
        @Override
        public int compare(@Nullable Object o, @Nullable Object t1) {
            return Doubles.compare(((Number) o).doubleValue(), ((Number) t1).doubleValue());
        }
    };

    private final FloatColumnSelector selector;
    private final String name;

    private double sum;

    public WeightedHyperUniqueAggregator(String name, FloatColumnSelector floatColumnSelector) {
        this.name = name;
        this.selector = floatColumnSelector;

        reset();
    }

    @Override
    public void aggregate() {
        sum += selector.get();
    }

    @Override
    public void reset() {
        sum = 0;
    }

    @Override
    public Object get() {
        return sum;
    }

    @Override
    public float getFloat() {
        return (float) sum;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void close() {

    }

    @Override
    public long getLong() {
        throw new UnsupportedOperationException("Weighted Unique Aggregator does not support getLong()");
    }

    public static double combine(Object lhs, Object rhs) {
        return ((Number) lhs).doubleValue() + ((Number) rhs).doubleValue();
    }
}
