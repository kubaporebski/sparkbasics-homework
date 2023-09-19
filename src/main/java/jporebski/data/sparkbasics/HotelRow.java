package jporebski.data.sparkbasics;

import ch.hsr.geohash.GeoHash;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.io.Serializable;

public class HotelRow implements Serializable {
    private long Id;
    private String Name;
    private String Country;
    private String City;
    private String Address;
    private double Latitude;
    private double Longitude;
    private String Geohash;

    public HotelRow() { }

    public HotelRow(long id, String name, String country, String city, String address, double latitude, double longitude) {
        Id = id;
        Name = name;
        Country = country;
        City = city;
        Address = address;
        Latitude = latitude;
        Longitude = longitude;
        Geohash = Utils.generateGeoHash(latitude, longitude);
    }

    public long getId() {
        return Id;
    }

    public void setId(long id) {
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

    public double getLatitude() {
        return Latitude;
    }

    public void setLatitude(double latitude) {
        Latitude = latitude;
    }

    public double getLongitude() {
        return Longitude;
    }

    public void setLongitude(double longitude) {
        Longitude = longitude;
    }

    public String getGeohash() {
        return Geohash;
    }

    public void setGeohash(String geohash) {
        Geohash = geohash;
    }

    /**
     * Map original Spark Dataset row into a custom more robust HotelRow.
     *
     * @param corrector - instance of a class which will be used to correct wrong GPS position data.
     *                  This could be a simple corrector for a dev environment, or OpenCage corrector when using on prod.
     * @return mapping function which should be used in a `map` method
     */
    public static MapFunction<Row, HotelRow> getMappingFunction(LatLonCorrector corrector) {

        return row -> {
            String lat = row.getString(5);
            String lon = row.getString(6);
            LatLon ll;

            if (LatLonCorrector.checkIfOK(lat, lon)) {
                ll = new LatLon(lat, lon);
            } else {
                ll = corrector.getByAddress(row.getString(4));
            }

            // Id,Name,Country,City,Address,Latitude,Longitude
            long id = Long.parseLong(row.getString(0));
            return new HotelRow(id, row.getString(1), row.getString(2), row.getString(3), row.getString(4), ll.getLatitude(), ll.getLongitude());
        };
    }

    @Override
    public String toString() {
         return String.format("[%d|%s|%s|%s|%s|%s]", Id, Name, Country, City, Address, Geohash);
    }
}
