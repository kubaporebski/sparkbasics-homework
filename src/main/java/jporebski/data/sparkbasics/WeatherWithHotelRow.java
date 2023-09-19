package jporebski.data.sparkbasics;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.io.Serializable;

public class WeatherWithHotelRow implements Serializable {
    private Long Id;
    private String Name;
    private String Country;
    private String City;
    private String Address;
    private Double Longitude;
    private Double Latitude;
    private String Geohash;
    private Double AverageTempF;
    private Double AverageTempC;
    private String ObservationDate;
    private Integer ObservationYear;
    private Integer ObservationMonth;
    private Integer ObservationDay;

    public WeatherWithHotelRow() { }

    /**
     * Map original Spark Dataset row into a custom more robust WeatherWithHotelRow.
     *
     * @return mapping function which should be used in a `map` method
     */
    public static MapFunction<Row, WeatherWithHotelRow> getMappingFunction() {
        return row -> {
            WeatherWithHotelRow newRow = new WeatherWithHotelRow();

            newRow.setId(row.getAs("id"));
            newRow.setName(row.getAs("name"));
            newRow.setCity(row.getAs("city"));
            newRow.setCountry(row.getAs("country"));
            newRow.setAddress(row.getAs("address"));

            newRow.setAverageTempC(row.getAs("averageTempC"));
            newRow.setAverageTempF(row.getAs("averageTempF"));

            newRow.setObservationDate(row.getAs("observationDate"));
            newRow.setObservationDay(row.getAs("observationDay"));
            newRow.setObservationMonth(row.getAs("observationMonth"));
            newRow.setObservationYear(row.getAs("observationYear"));

            newRow.setLatitude(row.getAs("latitude"));
            newRow.setLongitude(row.getAs("longitude"));
            newRow.setGeohash(row.getAs("geohash"));

            return newRow;
        };
    }

    public Long getId() {
        return Id;
    }

    public void setId(Long id) {
        Id = id;
    }

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public String getCountry() {
        return Country;
    }

    public void setCountry(String country) {
        Country = country;
    }

    public String getCity() {
        return City;
    }

    public void setCity(String city) {
        City = city;
    }

    public String getAddress() {
        return Address;
    }

    public void setAddress(String address) {
        Address = address;
    }

    public Double getLongitude() {
        return Longitude;
    }

    public void setLongitude(Double longitude) {
        Longitude = longitude;
    }

    public Double getLatitude() {
        return Latitude;
    }

    public void setLatitude(Double latitude) {
        Latitude = latitude;
    }

    public String getGeohash() {
        return Geohash;
    }

    public void setGeohash(String geohash) {
        Geohash = geohash;
    }

    public Double getAverageTempF() {
        return AverageTempF;
    }

    public void setAverageTempF(Double averageTempF) {
        AverageTempF = averageTempF;
    }

    public Double getAverageTempC() {
        return AverageTempC;
    }

    public void setAverageTempC(Double averageTempC) {
        AverageTempC = averageTempC;
    }

    public String getObservationDate() {
        return ObservationDate;
    }

    public void setObservationDate(String observationDate) {
        ObservationDate = observationDate;
    }

    public Integer getObservationYear() {
        return ObservationYear;
    }

    public void setObservationYear(Integer observationYear) {
        ObservationYear = observationYear;
    }

    public Integer getObservationMonth() {
        return ObservationMonth;
    }

    public void setObservationMonth(Integer observationMonth) {
        ObservationMonth = observationMonth;
    }

    public Integer getObservationDay() {
        return ObservationDay;
    }

    public void setObservationDay(Integer observationDay) {
        ObservationDay = observationDay;
    }
}
