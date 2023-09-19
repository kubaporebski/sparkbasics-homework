package jporebski.data.sparkbasics;

import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;

public class MainApplication {

    final static Logger log = LoggerFactory.getLogger(MainApplication.class);

    final static LatLonCorrector corrector = Utils.createLatLonCorrector();


    /**
     * Main entry point of the app doing following: <br>
     *
     * - Check hotels data on incorrect (null) values (Latitude & Longitude). For incorrect values map (Latitude & Longitude) from OpenCage Geocoding API in job on fly (Via REST API). <br>
     * - Generate geohash by Latitude & Longitude using one of geohash libraries (like geohash-java) with 4-characters length in extra column. <br>
     * - Left join weather and hotels data by generated 4-characters geohash (avoid data multiplication and make you job idempotent). <br>
     * - Deploy Spark job on Azure Kubernetes Service (AKS), to setup infrastructure use terraform scripts from module. For this use Running Spark on Kubernetes deployment guide and corresponding to your spark version docker image. Default resource parameters (specifically memory) will not work because of free tier limitations. You needed to setup memory and cores properly. <br>
     * - Development and testing is recommended to do locally in your IDE environment. <br>
     * - Store enriched data (joined data with all the fields from both datasets) in provisioned with terraform Azure ADLS gen2 storage preserving data partitioning in parquet format in “data” container (it marked with prevent_destroy=true and will survive terraform destroy). <br>
     *
     */
    public static void main(String[] args) {

        if (!Utils.loadAndValidate())
            return;

        try (SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("sparkbasics").getOrCreate()) {

            Dataset<HotelRow> hotelsDataset = loadHotelsDataset(sparkSession);
            log.info(String.format("Loaded %d HOTEL rows with schema %s", hotelsDataset.count(), hotelsDataset.schema()));

            Dataset<WeatherRow> weatherDataset = loadWeatherDataset(sparkSession);
            log.info(String.format("Loaded %d WEATHER rows with schema %s", weatherDataset.count(), weatherDataset.schema()));

            joinAndSave(hotelsDataset, weatherDataset);
            log.info("Weather data joined with hotel data by Geohash, written to a file.");
        }
    }

    /**
     * Here we have hotels data and weather data.
     * Let's join them together (weather left join hotels), remove duplicates, and save results to a one big parquet file.
     */
    private static void joinAndSave(Dataset<HotelRow> hotelsDataset, Dataset<WeatherRow> weatherDataset) {

        Seq<String> columns = JavaConverters.asScalaIteratorConverter(Arrays.asList("geohash", "geohash").iterator()).asScala().toSeq();
        Dataset<WeatherWithHotelRow> joined = weatherDataset.join(hotelsDataset, columns, "left")
                .dropDuplicates()
                .map(WeatherWithHotelRow.getMappingFunction(), Encoders.bean(WeatherWithHotelRow.class));

        // repartition(1) will make one single file
        joined.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(Utils.getDataDirectory("/joined"));
    }

    private static Dataset<WeatherRow> loadWeatherDataset(SparkSession sparkSession) {
        return sparkSession.read()
                .parquet(Utils.getDataDirectory("/weather"))
                .map(WeatherRow.getMappingFunction(), Encoders.bean(WeatherRow.class));
    }

    private static Dataset<HotelRow> loadHotelsDataset(SparkSession sparkSession) {
        return sparkSession.read()
                .format("csv")
                    .option("compression", "gzip")
                    .option("header", true)
                .load(Utils.getDataDirectory("/hotels"))
                .map(HotelRow.getMappingFunction(corrector), Encoders.bean(HotelRow.class));
    }
}
