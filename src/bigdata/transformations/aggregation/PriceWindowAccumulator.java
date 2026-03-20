package bigdata.transformations.aggregation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import bigdata.objects.StockPrice;

/**
 * Accumulator used with aggregateByKey to keep a bounded number of StockPrice items per ticker.
 *
 * Notes:
 * - We do NOT sort here (sorting per add is expensive).
 * - We DO trim to maxSize during merges to keep memory bounded and scalable.
 * - Final sort happens once in AccumulatorToFeaturesMap.
 */
public class PriceWindowAccumulator implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int maxSize;
    private final List<StockPrice> prices;

    public PriceWindowAccumulator(int maxSize) {
        this.maxSize = maxSize;
        this.prices = new ArrayList<StockPrice>();
    }

    public void add(StockPrice p) {
        prices.add(p);
        // No sorting here keeping it cheap
        // No trimming here either, trimming is done on merge
    }

    public void merge(PriceWindowAccumulator other) {
        prices.addAll(other.prices);

        // Keeping memory bounded even during shuffle merges
        // This is an approximate trim, not time-aware, until final sort in the map stage
        if (prices.size() > maxSize * 2) {
            // Drop some oldest-added elements cheaply
            // Final sort+trim will do the correct time-based trim
            int removeCount = prices.size() - (maxSize * 2);
            prices.subList(0, removeCount).clear();
        }
    }

    public int getMaxSize() {
        return maxSize;
    }

    public List<StockPrice> getPrices() {
        return prices;
    }
}