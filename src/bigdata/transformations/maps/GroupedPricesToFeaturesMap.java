package bigdata.transformations.maps;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.function.PairFunction;

import bigdata.objects.AssetFeatures;
import bigdata.objects.StockPrice;
import bigdata.technicalindicators.Returns;
import bigdata.technicalindicators.Volitility;
import bigdata.util.TimeUtil;
import scala.Tuple2;

/**
 * For each ticker, sorts prices by time, then computes:
 * - Returns over last numReturnDays (uses numReturnDays+1 closing prices)
 * - Volatility using last numVolatilityDays closing prices
 * If there isn't enough data, returns features with 0s (and later filters will drop them).
 */
public class GroupedPricesToFeaturesMap implements PairFunction<Tuple2<String, Iterable<StockPrice>>, String, AssetFeatures> {

    private static final long serialVersionUID = 1L;

    private final int numReturnDays;
    private final int numVolatilityDays;

    public GroupedPricesToFeaturesMap(int numReturnDays, int numVolatilityDays) {
        this.numReturnDays = numReturnDays;
        this.numVolatilityDays = numVolatilityDays;
    }

    @Override
    public Tuple2<String, AssetFeatures> call(Tuple2<String, Iterable<StockPrice>> grouped) {

        String ticker = grouped._1();
        Iterable<StockPrice> pricesIt = grouped._2();

        // Move to list so we can sort
        List<StockPrice> priceList = new ArrayList<StockPrice>();
        for (StockPrice p : pricesIt) {
            priceList.add(p);
        }

        // Sort ascending by date
        Collections.sort(priceList, new StockPriceTimeComparator());

        // Extract closing prices in time order
        List<Double> closes = new ArrayList<Double>(priceList.size());
        for (StockPrice p : priceList) {
            closes.add(p.getClosePrice());
        }

        AssetFeatures f = new AssetFeatures();

        // Need at least numReturnDays + 1 closes for Returns.calculate
        if (closes.size() >= (numReturnDays + 1)) {
            List<Double> returnWindow = closes.subList(closes.size() - (numReturnDays + 1), closes.size());
            double r = Returns.calculate(numReturnDays, returnWindow);
            f.setAssetReturn(r);
        } else {
            f.setAssetReturn(0d);
        }

        // Need at least numVolatilityDays closes for volatility (use last numVolatilityDays)
        if (closes.size() >= numVolatilityDays) {
            List<Double> volWindow = closes.subList(closes.size() - numVolatilityDays, closes.size());
            double v = Volitility.calculate(volWindow);
            f.setAssetVolitility(v);
        } else {
            f.setAssetVolitility(Double.POSITIVE_INFINITY); // ensures it will be filtered out
        }

        // P/E is filled after joining metadata
        f.setPeRatio(0d);

        return new Tuple2<String, AssetFeatures>(ticker, f);
    }

    /**
     * Comparator for StockPrice by date using TimeUtil.
     */
    private static class StockPriceTimeComparator implements Comparator<StockPrice>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(StockPrice a, StockPrice b) {
            Instant ta = TimeUtil.fromDate(a.getYear(), a.getMonth(), a.getDay());
            Instant tb = TimeUtil.fromDate(b.getYear(), b.getMonth(), b.getDay());
            return ta.compareTo(tb);
        }
    }
}