package jporebski.data.sparkbasics;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;

public class MainApplicationTest {

    /**
     * Directory where unit test will be storing temporary files.
     * This directory will get created and deleted after the unit test.
     *
     * You can provide your own directory via env variable TESTING_DIRECTORY.
     */
    static String temporaryTestDirectory = Optional.ofNullable(System.getenv("TESTING_DIRECTORY")).orElse("/tmp/m06sparkbasics-tests");

    /**
     * Bean for creation of one single testing row of weather data.
     */
    public static class WeatherParquetRow {
        private double lng;
        private double lat;
        private double avg_tmpr_f;
        private double avg_tmpr_c;
        private String wthr_date;
        private String day;
        private String month;
        private String year;

        public WeatherParquetRow() {

        }

        public WeatherParquetRow(double lng, double lat, double avg_tmpr_f, double avg_tmpr_c, String wthr_date, String day, String month, String year) {
            this.lng = lng;
            this.lat = lat;
            this.avg_tmpr_f = avg_tmpr_f;
            this.avg_tmpr_c = avg_tmpr_c;
            this.wthr_date = wthr_date;
            this.day = day;
            this.month = month;
            this.year = year;
        }

        public double getLng() {
            return lng;
        }

        public void setLng(double lng) {
            this.lng = lng;
        }

        public double getLat() {
            return lat;
        }

        public void setLat(double lat) {
            this.lat = lat;
        }

        public double getAvg_tmpr_f() {
            return avg_tmpr_f;
        }

        public void setAvg_tmpr_f(double avg_tmpr_f) {
            this.avg_tmpr_f = avg_tmpr_f;
        }

        public double getAvg_tmpr_c() {
            return avg_tmpr_c;
        }

        public void setAvg_tmpr_c(double avg_tmpr_c) {
            this.avg_tmpr_c = avg_tmpr_c;
        }

        public String getWthr_date() {
            return wthr_date;
        }

        public void setWthr_date(String wthr_date) {
            this.wthr_date = wthr_date;
        }

        public String getDay() {
            return day;
        }

        public void setDay(String day) {
            this.day = day;
        }

        public String getMonth() {
            return month;
        }

        public void setMonth(String month) {
            this.month = month;
        }

        public String getYear() {
            return year;
        }

        public void setYear(String year) {
            this.year = year;
        }
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        // 1. prepare a temporary directory and a path to a dummy GCP key (this file doesn't need to exist in the test)
        Files.createDirectories(Paths.get(temporaryTestDirectory));
        Utils.config.put("HOMEWORK_DATA_DIR", temporaryTestDirectory);
        Utils.config.put("GOOGLE_APPLICATION_CREDENTIALS", temporaryTestDirectory + "/dummy.gcp.key");

        // 2. create really simple CSV and parquet files that will be used in MainApplication
        // 2.1 save one record of a hotel to a csv file
        String hotelFileContents = "Id,Name,Country,City,Address,Latitude,Longitude\r\n" +
                "4,Holiday Inn,US,Warsaw,1 Main Street 00-000,12.34,56.78";
        Files.write(Paths.get(temporaryTestDirectory + "/hotels"), hotelFileContents.getBytes());

        try (SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("sparkbasics-test")
                .getOrCreate()) {

            // 2.2 save one record of a weather to a parquet file
            WeatherParquetRow weatherRow = new WeatherParquetRow(56.78, 12.34, 77, 25, "2020-01-22", "22", "01", "2020");
            sparkSession
                    .createDataset(Collections.singletonList(weatherRow), Encoders.bean(WeatherParquetRow.class))
                    .write()
                    .mode("overwrite")
                    .parquet(temporaryTestDirectory + "/weather");
        }
    }

    @AfterAll
    public static void afterAll()  {
        // clean up
        deleteDirectory(new File(temporaryTestDirectory));
    }

    @Test
    public void testApp() {
        /* Arrange */
        // Done in the `beforeAll` method

        /* Act */
        MainApplication.main(new String[0]);

        /* Assert */
        final String resultPath = temporaryTestDirectory + "/joined";
        File joined = new File(resultPath);
        Assertions.assertTrue(joined.exists(), "There should be a `joined` directory created");

        File[] results = joined.listFiles();
        Assertions.assertNotNull(results, "There should be files inside a `joined` directory");
        Assertions.assertTrue(results.length > 0, "There should be at least one file inside a `joined` directory");

        try (SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("sparkbasics-test")
                .getOrCreate()) {

            Dataset<Row> resultRows = sparkSession.read().parquet(resultPath);
            Assertions.assertEquals(1, resultRows.count(), "There should be only one row inside resulting parquet file");

            // and check values of a few fields
            WeatherWithHotelRow resultRow = resultRows.map(WeatherWithHotelRow.getMappingFunction(), Encoders.bean(WeatherWithHotelRow.class)).takeAsList(1).get(0);
            Assertions.assertEquals("Holiday Inn", resultRow.getName());
            Assertions.assertEquals("1 Main Street 00-000", resultRow.getAddress());
            Assertions.assertEquals(25, resultRow.getAverageTempC());
            Assertions.assertEquals("2020-01-22", resultRow.getObservationDate());
        }
    }

    // following code got from https://www.baeldung.com/java-delete-directory
    private static void deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directoryToBeDeleted.delete();
    }
}
