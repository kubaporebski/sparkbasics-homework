package jporebski.data.sparkbasics;

public class LatLon {
    final String latitude;

    final String longitude;

    public final static LatLon EMPTY = new LatLon("0", "0");

    public LatLon(String latitude, String longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public double getLatitude() {
        return Double.parseDouble(latitude);
    }

    public double getLongitude() {
        return Double.parseDouble(longitude);
    }

    @Override
    public String toString() {
        return String.format("[%s, %s]", latitude, longitude);
    }
}
