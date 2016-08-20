package io.druid.query.aggregation.weightedHyperUnique;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.ObjectColumnSelector;

import javax.annotation.Nullable;
import java.util.Comparator;

/**
 * Created by sajo on 2/8/16.
 */
public class WeightedHyperUniqueAggregator implements Aggregator {

    public static final Comparator COMPARATOR = new Ordering() {
        @Override
        public int compare(@Nullable Object o, @Nullable Object t1) {
            return Floats.compare(((WeightedHyperUnique) o).getValue(), ((WeightedHyperUnique) t1).getValue());
        }
    };

    private final ObjectColumnSelector<WeightedHyperUnique> selector;
    private final String name;

    private WeightedHyperUnique sum;

    public WeightedHyperUniqueAggregator(String name, ObjectColumnSelector<WeightedHyperUnique> columnSelector) {
        this.name = name;
        this.selector = columnSelector;

        reset();
    }

    @Override
    public void aggregate() {
        sum.fold(selector.get());
    }

    @Override
    public void reset() {
        sum = new WeightedHyperUnique(0);
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

    public static WeightedHyperUnique combine(Object lhs, Object rhs) {
        if (lhs.getClass() != WeightedHyperUnique.class || rhs.getClass() != WeightedHyperUnique.class){
            throw new UnsupportedOperationException("Can only combine weighted hyper uniques");
        }

        WeightedHyperUnique w1 = (WeightedHyperUnique)lhs;
        WeightedHyperUnique w2 = (WeightedHyperUnique)rhs;

        return w1.fold(w2);
    }
}
