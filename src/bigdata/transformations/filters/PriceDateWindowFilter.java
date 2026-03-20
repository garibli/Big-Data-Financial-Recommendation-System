package bigdata.transformations.filters;

import java.time.Instant;

import org.apache.spark.api.java.function.Function;

import bigdata.objects.StockPrice;
import bigdata.util.TimeUtil;

/**
 * Filters StockPrice records to those within [start, end] inclusive.
 */

public class PriceDateWindowFilter implements Function<StockPrice, Boolean> {

    private static final long serialVersionUID = 1L;

    private final Instant startInclusive;
    private final Instant endInclusive;

    public PriceDateWindowFilter(Instant startInclusive, Instant endInclusive) {
        this.startInclusive = startInclusive;
        this.endInclusive = endInclusive;
    }

    @Override
    public Boolean call(StockPrice p) {
        Instant t = TimeUtil.fromDate(p.getYear(), p.getMonth(), p.getDay());
        return !t.isBefore(startInclusive) && !t.isAfter(endInclusive);
    }
}