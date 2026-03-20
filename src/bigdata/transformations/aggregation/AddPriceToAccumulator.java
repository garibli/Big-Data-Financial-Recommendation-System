package bigdata.transformations.aggregation;

import org.apache.spark.api.java.function.Function2;

import bigdata.objects.StockPrice;

/**
 * SeqOp for aggregateByKey: adds one StockPrice into the accumulator.
 */
public class AddPriceToAccumulator implements Function2<PriceWindowAccumulator, StockPrice, PriceWindowAccumulator> {

    private static final long serialVersionUID = 1L;

    @Override
    public PriceWindowAccumulator call(PriceWindowAccumulator acc, StockPrice p) {
        acc.add(p);
        return acc;
    }
}