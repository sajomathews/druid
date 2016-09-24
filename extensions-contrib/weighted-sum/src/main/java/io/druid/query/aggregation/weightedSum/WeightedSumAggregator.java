package io.druid.query.aggregation.weightedSum;

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
            return Floats.compare(((WeightedSum) o).getValue(), ((WeightedSum) t1).getValue());
        }
    };

    private final ObjectColumnSelector selector;
    private final String name;

    private WeightedSum sum;

    public WeightedSumAggregator(String name, ObjectColumnSelector columnSelector) {
        this.name = name;
        this.selector = columnSelector;
        reset();
    }

    @Override
    public void aggregate() {
        sum.fold((WeightedSum)selector.get());
    }

    @Override
    public void reset() {
        sum = new WeightedSum(0);
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
        throw new UnsupportedOperationException("Weighted Sum does not support getLong()");
        //TODO: This should support getLong
    }

    public static WeightedSum combine(Object lhs, Object rhs) {
        if (lhs.getClass() != WeightedSum.class || rhs.getClass() != WeightedSum.class){
            throw new UnsupportedOperationException("Can only combine weighted sums");
            //TODO: Should be able to combine any long / float as well
        }

        WeightedSum w1 = (WeightedSum)lhs;
        WeightedSum w2 = (WeightedSum)rhs;

        return w1.fold(w2);
    }
}
