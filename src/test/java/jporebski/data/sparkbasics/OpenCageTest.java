package jporebski.data.sparkbasics;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class OpenCageTest {

    /**
     * This test suite requires environment variable OPENCAGE_API_KEY to be provided.
     * This variable stores OpenCage API key.
     */
    @BeforeAll
    public static void beforeAll() {
        String apiKey = System.getenv("OPENCAGE_API_KEY");
        assertTrue(apiKey != null && !apiKey.isEmpty());
    }

    /**
     * Following test will check if connection to OpenCage API works correcly, and what's more,
     *   if retrieved data is parsed correcly, and if results are OK.
     * This is happy path test.
     */
    @Test
    public void testDataRetrieval() {
        // Arrange
        String address = "Kolejowa 1, Krakow, Poland";

        // Act
        OpenCageLatLonCorrector corrector = new OpenCageLatLonCorrector();
        LatLon actual = corrector.getByAddress(address);

        // Assert: resulting coords should be about around 20E, 50N
        assertNotNull(actual);
        assertNotEquals(LatLon.EMPTY, actual);
        assertEquals(50.0, Math.round(actual.getLatitude()));
        assertEquals(20.0, Math.round(actual.getLongitude()));
        System.out.println(actual);
    }

}
