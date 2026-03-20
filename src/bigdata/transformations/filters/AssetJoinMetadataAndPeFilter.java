package bigdata.transformations.filters;

import org.apache.spark.api.java.function.Function;

import bigdata.objects.AssetFeatures;
import bigdata.objects.AssetMetadata;
import scala.Tuple2;

/**
 * Filters joined (features, metadata) to enforce:
 * - metadata must exist and contain required fields
 * - P/E ratio must be < threshold
 * - P/E ratio must be > 0 (treat 0 as "missing" in this dataset)
 */
public class AssetJoinMetadataAndPeFilter implements Function<Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>>, Boolean> {

    private static final long serialVersionUID = 1L;

    private final double peThreshold;

    public AssetJoinMetadataAndPeFilter(double peThreshold) {
        this.peThreshold = peThreshold;
    }

    @Override
    public Boolean call(Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>> joined) {
        AssetMetadata m = joined._2()._2();

        if (m == null) return false;
        if (m.getName() == null) return false;
        if (m.getIndustry() == null) return false;
        if (m.getSector() == null) return false;

        double pe = m.getPriceEarningRatio();
        if (pe <= 0d) return false;
        if (pe >= peThreshold) return false;

        return true;
    }
}