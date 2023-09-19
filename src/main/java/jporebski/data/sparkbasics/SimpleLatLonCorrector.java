package jporebski.data.sparkbasics;

import java.io.Serializable;

public class SimpleLatLonCorrector extends LatLonCorrector implements Serializable {
    @Override
    public LatLon getByAddress(String address) {
        return new LatLon("50", "20");
    }
}
