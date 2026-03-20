package bigdata.transformations.filters;

import org.apache.spark.api.java.function.Function;

import bigdata.objects.AssetFeatures;
import scala.Tuple2;

/**
 * Keeps assets with volatility strictly less than the ceiling.
 * (We remove volatility >= ceiling.)
 */

public class AssetVolatilityCeilingFilter implements Function<Tuple2<String, AssetFeatures>, Boolean> {

    private static final long serialVersionUID = 1L;

    private final double ceiling;

    public AssetVolatilityCeilingFilter(double ceiling) {
        this.ceiling = ceiling;
    }

    @Override
    public Boolean call(Tuple2<String, AssetFeatures> t) {
        AssetFeatures f = t._2();
        return f.getAssetVolitility() < ceiling;
    }
}