package bigdata.transformations.aggregation;

import org.apache.spark.api.java.function.Function2;

/**
 * CombOp for aggregateByKey: merges two accumulators.
 */
public class MergeAccumulators implements Function2<PriceWindowAccumulator, PriceWindowAccumulator, PriceWindowAccumulator> {

    private static final long serialVersionUID = 1L;

    @Override
    public PriceWindowAccumulator call(PriceWindowAccumulator a, PriceWindowAccumulator b) {
        a.merge(b);
        return a;
    }
}