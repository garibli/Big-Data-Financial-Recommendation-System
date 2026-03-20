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
import bigdata.transformations.aggregation.PriceWindowAccumulator;
import bigdata.util.TimeUtil;
import scala.Tuple2;

/**
 * Converts a PriceWindowAccumulator into AssetFeatures (return + volatility).
 * We sort ONCE per ticker (fast) and then trim to the most recent N by time (correct).
 */
public class AccumulatorToFeaturesMap implements PairFunction<Tuple2<String, PriceWindowAccumulator>, String, AssetFeatures> {

    private static final long serialVersionUID = 1L;

    private final int returnDays;
    private final int volatilityDays;

    public AccumulatorToFeaturesMap(int returnDays, int volatilityDays) {
        this.returnDays = returnDays;
        this.volatilityDays = volatilityDays;
    }

    @Override
    public Tuple2<String, AssetFeatures> call(Tuple2<String, PriceWindowAccumulator> input) {

        String ticker = input._1();
        PriceWindowAccumulator acc = input._2();

        // Copy so we can sort/trim safely
        List<StockPrice> priceList = new ArrayList<StockPrice>(acc.getPrices());

        // Sort ascending by time once (correctness)
        Collections.sort(priceList, new StockPriceTimeComparator());

        // Keep only the most recent maxSize by time (correct trimming)
        int maxSize = acc.getMaxSize();
        if (priceList.size() > maxSize) {
            priceList = priceList.subList(priceList.size() - maxSize, priceList.size());
        }

        // Extract closes in time order
        List<Double> closes = new ArrayList<Double>(priceList.size());
        for (StockPrice p : priceList) {
            closes.add(p.getClosePrice());
        }

        AssetFeatures f = new AssetFeatures();

        // Returns: need returnDays + 1 closes
        if (closes.size() >= returnDays + 1) {
            List<Double> retWindow = closes.subList(closes.size() - (returnDays + 1), closes.size());
            f.setAssetReturn(Returns.calculate(returnDays, retWindow));
        } else {
            f.setAssetReturn(0d);
        }

        // Volatility: need volatilityDays closes
        if (closes.size() >= volatilityDays) {
            List<Double> volWindow = closes.subList(closes.size() - volatilityDays, closes.size());
            f.setAssetVolitility(Volitility.calculate(volWindow));
        } else {
            f.setAssetVolitility(Double.POSITIVE_INFINITY);
        }

        // P/E is filled after join with metadata
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