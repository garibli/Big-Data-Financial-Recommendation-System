package bigdata.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.Instant;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import bigdata.objects.Asset;
import bigdata.objects.AssetFeatures;
import bigdata.objects.AssetMetadata;
import bigdata.objects.AssetRanking;
import bigdata.objects.StockPrice;
import bigdata.transformations.comparators.AssetReturnComparatorDesc;
import bigdata.transformations.filters.AssetJoinMetadataAndPeFilter;
import bigdata.transformations.filters.AssetVolatilityCeilingFilter;
import bigdata.transformations.filters.NullPriceFilter;
import bigdata.transformations.filters.PriceDateWindowFilter;
import bigdata.transformations.maps.AssetFromJoinMap;
import bigdata.transformations.maps.PriceReaderMap;
import bigdata.transformations.maps.StockPriceToTickerPairMap;
import bigdata.transformations.pairing.AssetMetadataPairing;
import bigdata.util.TimeUtil;
import bigdata.transformations.aggregation.AddPriceToAccumulator;
import bigdata.transformations.aggregation.MergeAccumulators;
import bigdata.transformations.aggregation.PriceWindowAccumulator;
import bigdata.transformations.maps.AccumulatorToFeaturesMap;

import scala.Tuple2;

public class AssessedExercise {

    public static void main(String[] args) throws InterruptedException {

        //--------------------------------------------------------
        // Static Configuration
        //--------------------------------------------------------
        String datasetEndDate = "2020-04-01";
        double volatilityCeiling = 4;
        double peRatioThreshold = 25;

        long startTime = System.currentTimeMillis();

        // The code submitted for the assessed exerise may be run in either local or remote modes
        // Configuration of this will be performed based on an environment variable
        String sparkMasterDef = System.getenv("SPARK_MASTER");
        if (sparkMasterDef == null) {
            File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
            System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
            sparkMasterDef = "local[4]"; // default is local mode with two executors
        }

        String sparkSessionName = "BigDataAE"; // give the session a name

        // Create the Spark Configuration
        SparkConf conf = new SparkConf()
                .setMaster(sparkMasterDef)
                .setAppName(sparkSessionName);

        // Create the spark session
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();


        // Get the location of the asset pricing data
        String pricesFile = System.getenv("BIGDATA_PRICES");
        if (pricesFile == null) pricesFile = "resources/all_prices-noHead.csv"; // default is a sample with 3 queries

        // Get the asset metadata
        String assetsFile = System.getenv("BIGDATA_ASSETS");
        if (assetsFile == null) assetsFile = "resources/stock_data.json"; // default is a sample with 3 queries


        //----------------------------------------
        // Pre-provided code for loading the data
        //----------------------------------------

        // Load in the assets, this is a relatively small file
        Dataset<Row> assetRows = spark.read().option("multiLine", true).json(assetsFile);
        System.err.println(assetRows.first().toString());
        JavaPairRDD<String, AssetMetadata> assetMetadata = assetRows.toJavaRDD().mapToPair(new AssetMetadataPairing());

        // Load in the prices, this is a large file (not so much in data size, but in number of records)
        Dataset<Row> priceRows = spark.read().csv(pricesFile); // read CSV file
        Dataset<Row> priceRowsNoNull = priceRows.filter(new NullPriceFilter()); // filter out rows with null prices
        Dataset<StockPrice> prices = priceRowsNoNull.map(new PriceReaderMap(), Encoders.bean(StockPrice.class)); // Convert to Stock Price Objects


        AssetRanking finalRanking = rankInvestments(spark, assetMetadata, prices, datasetEndDate, volatilityCeiling, peRatioThreshold);

        System.out.println(finalRanking.toString());

        System.out.println("Holding Spark UI open for 1 minute: http://localhost:4040");

        Thread.sleep(60000);

        // Close the spark session
        spark.close();

        String out = System.getenv("BIGDATA_RESULTS");
        String resultsDIR = "results/";
        if (out != null) resultsDIR = out;


        long endTime = System.currentTimeMillis();

        try {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(resultsDIR).getAbsolutePath() + "/SPARK.DONE")));

            Instant sinstant = Instant.ofEpochSecond(startTime / 1000);
            Date sdate = Date.from(sinstant);

            Instant einstant = Instant.ofEpochSecond(endTime / 1000);
            Date edate = Date.from(einstant);

            writer.write("StartTime:" + sdate.toGMTString() + '\n');
            writer.write("EndTime:" + edate.toGMTString() + '\n');
            writer.write("Seconds: " + ((endTime - startTime) / 1000) + '\n');
            writer.write('\n');
            writer.write(finalRanking.toString());
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static AssetRanking rankInvestments(
            SparkSession spark,
            JavaPairRDD<String, AssetMetadata> assetMetadata,
            Dataset<StockPrice> prices,
            String datasetEndDate,
            double volatilityCeiling,
            double peRatioThreshold) {

        // -----------------------------
        // 1) Restrict prices to a time window ending at datasetEndDate
        //    (performance critical: avoids grouping decades of data)
        // -----------------------------
        Instant end = TimeUtil.fromDate(datasetEndDate);

        // 251 trading days ~ ~1 year; use a safe calendar buffer so we still have 251 trading days
        Instant start = end.minusSeconds(400L * 24L * 60L * 60L); // 400 days buffer

        JavaRDD<StockPrice> priceRDD = prices.toJavaRDD();
        JavaRDD<StockPrice> windowedPrices = priceRDD.filter(new PriceDateWindowFilter(start, end));

        // -----------------------------
        // 2) Key by ticker and group prices per asset
        // -----------------------------
        JavaPairRDD<String, StockPrice> byTicker = windowedPrices.mapToPair(new StockPriceToTickerPairMap());

        // -----------------------------
        // 3) Compute indicators (returns over 5 days, volatility over 251 days)
        // -----------------------------
        // Keep only the most recent 252 prices per ticker (251 for volatility + 1 extra for returns window)
        JavaPairRDD<String, PriceWindowAccumulator> accByTicker =
                byTicker.aggregateByKey(
                        new PriceWindowAccumulator(252),
                        new AddPriceToAccumulator(),
                        new MergeAccumulators()
                );

        JavaPairRDD<String, AssetFeatures> featuresByTicker =
                accByTicker.mapToPair(new AccumulatorToFeaturesMap(5, 251));

        // Filter out assets with volatility >= ceiling
        JavaPairRDD<String, AssetFeatures> lowVolFeatures =
                featuresByTicker.filter(new AssetVolatilityCeilingFilter(volatilityCeiling));

        // -----------------------------
        // 4) Join with metadata, filter P/E and missing required metadata fields
        // -----------------------------
        JavaPairRDD<String, Tuple2<AssetFeatures, AssetMetadata>> joined =
                lowVolFeatures.join(assetMetadata);

        JavaPairRDD<String, Tuple2<AssetFeatures, AssetMetadata>> fundamentalsFiltered =
                joined.filter(new AssetJoinMetadataAndPeFilter(peRatioThreshold));

        // -----------------------------
        // 5) Build Asset objects and take top-5 by returns
        // -----------------------------
        JavaRDD<Asset> assets = fundamentalsFiltered.map(new AssetFromJoinMap());

        List<Asset> top5 = assets.takeOrdered(5, new AssetReturnComparatorDesc());

        Asset[] ranked = new Asset[5];
        for (int i = 0; i < 5; i++) {
            ranked[i] = (i < top5.size()) ? top5.get(i) : null;
        }

        return new AssetRanking(ranked);
    }
}