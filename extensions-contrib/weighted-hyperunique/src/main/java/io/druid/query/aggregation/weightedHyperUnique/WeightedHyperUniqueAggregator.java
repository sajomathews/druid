package io.druid.query.aggregation.weightedHyperUnique;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.timeline.partition.IntegerPartitionChunk;

import javax.annotation.Nullable;
import java.util.Comparator;

/**
 * Created by sajo on 2/8/16.
 */
public class WeightedHyperUniqueAggregator implements Aggregator {

    private final ObjectColumnSelector selector;
    private final String name;

    private HyperLogLogCollector collector;

    public WeightedHyperUniqueAggregator(String name, ObjectColumnSelector columnSelector) {
        this.name = name;
        this.selector = columnSelector;
        this.collector = HyperLogLogCollector.makeLatestCollector();
    }

    @Override
    public void aggregate() {
        collector.fold((HyperLogLogCollector) selector.get());
    }

    @Override
    public void reset() {
        collector = HyperLogLogCollector.makeLatestCollector();
    }

    @Override
    public Object get() {
        return collector;
    }

    @Override
    public float getFloat() {
        throw new UnsupportedOperationException("HyperUniquesAggregator does not support getFloat()");
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

    @Override
    public Aggregator clone()
    {
        return new WeightedHyperUniqueAggregator(name, selector);
    }

}
