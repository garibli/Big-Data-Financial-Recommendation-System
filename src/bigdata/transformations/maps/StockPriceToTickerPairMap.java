package bigdata.transformations.maps;

import org.apache.spark.api.java.function.PairFunction;

import bigdata.objects.StockPrice;
import scala.Tuple2;

/**
 * Pair StockPrice records by stock ticker.
 */
public class StockPriceToTickerPairMap implements PairFunction<StockPrice, String, StockPrice> {

    private static final long serialVersionUID = 1L;

    @Override
    public Tuple2<String, StockPrice> call(StockPrice p) {
        return new Tuple2<String, StockPrice>(p.getStockTicker(), p);
    }
}