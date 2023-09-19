package jporebski.data.sparkbasics;

import ch.hsr.geohash.GeoHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public final class Utils {

    final static Logger log = LoggerFactory.getLogger(Utils.class);

    static String dataDirectory;

    /**
     * Load important information from environment variables and check if it's correct.
     * @return true - ok, the app can proceed / false - something is wrong
     */
    public static boolean loadAndValidate() {

        dataDirectory = System.getenv("HOMEWORK_DATA_DIR");
        if (dataDirectory == null) {
            log.error("No environment variable HOMEWORK_DATA_DIR supplied!");
            return false;
        }

        return true;
    }

    public static String getDataDirectory(String appendPathPart) {
        return dataDirectory + appendPathPart;
    }

    /**
     * Generation of 4-character geohash for given coordinates.
     * @param latitude
     * @param longitude
     * @return 4-character string
     */
    public static String generateGeoHash(double latitude, double longitude) {
        return GeoHash.withCharacterPrecision(latitude, longitude, 4).toBase32();
    }

    /**
     * Important bit of the app. Here we try to load the right kind of Latitude & Longitude corrector mechanism.
     * For local testing, we don't want to use that OpenCage, because it has its limits (1000 requests per day or sth like that),
     *   so then we use `SimpleLatLonCorrector` that returns some dummy value when lat & lon data is wrong.
     * On prod instead, we can use corrector that connects to OpenCage API.
     *
     * @return an instance of LatLonCorrector class
     */
    public static LatLonCorrector createLatLonCorrector() {
        try {
            String simpleClassName = Objects.toString(System.getenv("LATLON_CORRECTOR"), "SimpleLatLonCorrector");
            Class<? extends LatLonCorrector> cls = (Class<? extends LatLonCorrector>) Class.forName("jporebski.data.sparkbasics." + simpleClassName);
            return cls.newInstance();
        }
        catch (Exception ex) {
            log.error("Error creating LatLonCorrector");
            log.error(ex.toString());

            // throw the exception forward, because MainApplication has to fail in this situation
            throw new RuntimeException(ex);
        }
    }
}
