package bigdata.transformations.comparators;

import java.io.Serializable;
import java.util.Comparator;

import bigdata.objects.Asset;

/**
 * Sort Assets by returns descending (highest return first).
 */
public class AssetReturnComparatorDesc implements Comparator<Asset>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public int compare(Asset a, Asset b) {
        // Reverse order: b vs a
        return Double.compare(b.getFeatures().getAssetReturn(), a.getFeatures().getAssetReturn());
    }
}