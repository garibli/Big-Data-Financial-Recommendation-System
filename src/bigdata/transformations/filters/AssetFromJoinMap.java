package bigdata.transformations.maps;

import org.apache.spark.api.java.function.Function;

import bigdata.objects.Asset;
import bigdata.objects.AssetFeatures;
import bigdata.objects.AssetMetadata;
import scala.Tuple2;

/**
 * Converts joined (ticker -> (features, metadata)) into an Asset object.
 */

public class AssetFromJoinMap implements Function<Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>>, Asset> {

    private static final long serialVersionUID = 1L;

    @Override
    public Asset call(Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>> joined) {
        String ticker = joined._1();
        AssetFeatures f = joined._2()._1();
        AssetMetadata m = joined._2()._2();

        // set P/E into features so it's recorded in final output
        f.setPeRatio(m.getPriceEarningRatio());

        return new Asset(ticker, f, m.getName(), m.getIndustry(), m.getSector());
    }
}