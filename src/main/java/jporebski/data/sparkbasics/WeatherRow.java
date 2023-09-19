package jporebski.data.sparkbasics;

import ch.hsr.geohash.GeoHash;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Objects;

public class WeatherRow implements Serializable {

    private double Longitude;
    private double Latitude;
    private String Geohash;
    private double AverageTempF;
    private double AverageTempC;
    private String ObservationDate;
    private int ObservationYear;
    private int ObservationMonth;
    private int ObservationDay;

    public WeatherRow(double longitude, double latitude, double averageTempF, double averageTempC, String observationDate, int observationYear, int observationMonth, int observationDay) {
        Longitude = longitude;
        Latitude = latitude;
        AverageTempF = averageTempF;
        AverageTempC = averageTempC;
        ObservationDate = observationDate;
        ObservationYear = observationYear;
        ObservationMonth = observationMonth;
        ObservationDay = observationDay;
        Geohash = Utils.generateGeoHash(latitude, longitude);
    }

    public double getLongitude() {
        return Longitude;
    }

    public void setLongitude(double longitude) {
        Longitude = longitude;
    }

    public double getLatitude() {
        return Latitude;
    }

    public void setLatitude(double latitude) {
        Latitude = latitude;
    }

    public String getGeohash() {
        return Geohash;
    }

    public void setGeohash(String geohash) {
        Geohash = geohash;
    }

    public double getAverageTempF() {
        return AverageTempF;
    }

    public void setAverageTempF(double averageTempF) {
        AverageTempF = averageTempF;
    }

    public double getAverageTempC() {
        return AverageTempC;
    }

    public void setAverageTempC(double averageTempC) {
        AverageTempC = averageTempC;
    }

    public String getObservationDate() {
        return ObservationDate;
    }

    public void setObservationDate(String observationDate) {
        ObservationDate = observationDate;
    }

    public int getObservationYear() {
        return ObservationYear;
    }

    public void setObservationYear(int observationYear) {
        ObservationYear = observationYear;
    }

    public int getObservationMonth() {
        return ObservationMonth;
    }

    public void setObservationMonth(int observationMonth) {
        ObservationMonth = observationMonth;
    }

    public int getObservationDay() {
        return ObservationDay;
    }

    public void setObservationDay(int observationDay) {
        ObservationDay = observationDay;
    }

        /*| Column name | Description | Data type | Partition |
| --- | --- | --- | --- |
| lng | Longitude of a weather station | double | no |
| lat | Latitude of a weather station | double | no |
| avg_tmpr_f | Average temperature in Fahrenheit | double | no |
| avg_tmpr_c | Average temperature in Celsius | double | no |
| wthr_date | Date of observation (YYYY-MM-DD) | string | no |
| wthr_year | Year of observation (YYYY) | string | yes |
| wthr_month | Month of observation (MM) | string | yes |
| wthr_day | Day of observation (DD) | string | yes |*/

    public static MapFunction<Row, WeatherRow> getMappingFunction() {
        return row -> {

            int obsYear = Integer.parseInt(Objects.toString(row.getAs("year"), "0")),
                    obsMonth = Integer.parseInt(Objects.toString(row.getAs("month"), "1")),
                    obsDay = Integer.parseInt(Objects.toString(row.getAs("day"), "1"));

            return new WeatherRow(
                    row.getAs("lng"),
                    row.getAs("lat"),
                    row.getAs("avg_tmpr_f"),
                    row.getAs("avg_tmpr_c"),
                    row.getAs("wthr_date"),
                    obsYear, obsMonth, obsDay);
        };
    }
}
