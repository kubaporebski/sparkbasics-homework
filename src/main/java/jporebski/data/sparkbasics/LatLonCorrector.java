package jporebski.data.sparkbasics;

import java.io.Serializable;

/**
 * Abstract class defining base behaviour of correcting wrong latitude and longitude coordinates.
 *
 */
public abstract class LatLonCorrector implements Serializable {

    /**
     * Returns geographical coordinates for a given address.
     *
     * @param address
     * @return
     */
    public abstract LatLon getByAddress(String address);


    /**
     * Check if provided latitude and longitude are in the proper format.
     *
     * @param latitude
     * @param longitude
     * @return false - if there is something wrong with any of provided arguments /
     *         true - everything is OK
     */
    public static boolean checkIfOK(String latitude, String longitude) {
        if (latitude == null || longitude == null)
            return false;

        try {
            Double.parseDouble(latitude);
            Double.parseDouble(longitude);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
}
