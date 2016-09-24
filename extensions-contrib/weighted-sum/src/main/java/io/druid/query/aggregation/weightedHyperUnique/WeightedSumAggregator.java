package io.druid.query.aggregation.weightedHyperUnique;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Floats;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

import javax.annotation.Nullable;
import java.util.Comparator;

/**
 * Created by sajo on 2/8/16.
 */
public class WeightedSumAggregator implements Aggregator {

    public static final Comparator COMPARATOR = new Ordering() {
        @Override
        public int compare(@Nullable Object o, @Nullable Object t1) {
            return Floats.compare(((WeightedSumUnique) o).getValue(), ((WeightedSumUnique) t1).getValue());
        }
    };

    private final ObjectColumnSelector selector;
    private final String name;

    private WeightedSumUnique sum;

    public WeightedSumAggregator(String name, ObjectColumnSelector columnSelector) {
        this.name = name;
        this.selector = columnSelector;
        reset();
    }

    @Override
    public void aggregate() {
        sum.fold((WeightedSumUnique)selector.get());
    }

    @Override
    public void reset() {
        sum = new WeightedSumUnique(0);
    }

    @Override
    public Object get() {
        return sum;
    }

    @Override
    public float getFloat() {
        return sum.getValue();
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

    public static WeightedSumUnique combine(Object lhs, Object rhs) {
        if (lhs.getClass() != WeightedSumUnique.class || rhs.getClass() != WeightedSumUnique.class){
            throw new UnsupportedOperationException("Can only combine weighted hyper uniques");
        }

        WeightedSumUnique w1 = (WeightedSumUnique)lhs;
        WeightedSumUnique w2 = (WeightedSumUnique)rhs;

        return w1.fold(w2);
    }
}
