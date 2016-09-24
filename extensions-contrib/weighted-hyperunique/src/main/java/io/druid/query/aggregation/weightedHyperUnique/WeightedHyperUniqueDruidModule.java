package io.druid.query.aggregation.weightedHyperUnique;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;
import io.druid.segment.serde.ComplexMetrics;

import java.util.List;

/**
 * Created by sajo on 2/8/16.
 */
public class WeightedHyperUniqueDruidModule implements DruidModule{

    public static final String AGGREGATOR_TYPE = "weightedHyperUnique";
    @Override
    public List<? extends Module> getJacksonModules() {
        return ImmutableList.of(
                new SimpleModule().registerSubtypes(
                        WeightedHyperUniqueAggregatorFactory.class
                )
        );
    }

    @Override
    public void configure(Binder binder) {
        if (ComplexMetrics.getSerdeForType(AGGREGATOR_TYPE) == null) {
            ComplexMetrics.registerSerde(AGGREGATOR_TYPE, new WeightedHyperUniqueSerde(Hashing.murmur3_128()));
        }

    }
}
