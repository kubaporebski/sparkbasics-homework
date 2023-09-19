package jporebski.data.sparkbasics;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.Objects;

public class OpenCageLatLonCorrector extends LatLonCorrector implements Serializable {

    private final static String URL_FORMAT = "https://api.opencagedata.com/geocode/v1/json?q=%s&key=%s&language=en&pretty=1";

    private static String apiKey;

    @Override
    public LatLon getByAddress(String address) {
        try {
            URL url = new URL(String.format(URL_FORMAT, encodeURLComponent(address), getApiKey()));
            URLConnection connection = url.openConnection();
            try (InputStreamReader isr = new InputStreamReader(connection.getInputStream()); BufferedReader br = new BufferedReader(isr)) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
                String result = sb.toString();
                JSONObject ob = new JSONObject(result);
                return extractLocationFromJson(ob);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String encodeURLComponent(String address) {
        try {
            return URLEncoder.encode(address, "UTF-8");
        } catch (Exception ex) {
            return address;
        }
    }

    private LatLon extractLocationFromJson(JSONObject ob) {
        /* Sample result:
{
   "documentation" : "https://opencagedata.com/api",
   "licenses" : [
      {
         "name" : "see attribution guide",
         "url" : "https://opencagedata.com/credits"
      }
   ],
   "rate" : {
      "limit" : 2500,
      "remaining" : 2499,
      "reset" : 1691020800
   },
   "results" : [
      {
         "annotations" : {
            "DMS" : {
               "lat" : "50\u00b0 12' 35.64432'' N",
               "lng" : "19\u00b0 14' 48.90516'' E"
            },
            "MGRS" : "34UCA7491363439",
            "Maidenhead" : "JO90of90pj",
            "Mercator" : {
               "x" : 2142557.122,
               "y" : 6449854.72
            },
            "NUTS" : {
               "NUTS0" : {
                  "code" : "PL"
               },
               "NUTS1" : {
                  "code" : "PL2"
               },
               "NUTS2" : {
                  "code" : "PL22"
               },
               "NUTS3" : {
                  "code" : "PL22B"
               }
            },
            "OSM" : {
               "edit_url" : "https://www.openstreetmap.org/edit?way=208309391#map=16/50.20990/19.24692",
               "note_url" : "https://www.openstreetmap.org/note/new#map=16/50.20990/19.24692&layers=N",
               "url" : "https://www.openstreetmap.org/?mlat=50.20990&mlon=19.24692#map=16/50.20990/19.24692"
            },
            "UN_M49" : {
               "regions" : {
                  "EASTERN_EUROPE" : "151",
                  "EUROPE" : "150",
                  "PL" : "616",
                  "WORLD" : "001"
               },
               "statistical_groupings" : [
                  "MEDC"
               ]
            },
            "callingcode" : 48,
            "currency" : {
               "alternate_symbols" : [],
               "decimal_mark" : ",",
               "format" : "%n %u",
               "html_entity" : "z&#322;",
               "iso_code" : "PLN",
               "iso_numeric" : "985",
               "name" : "Polish Z\u0142oty",
               "smallest_denomination" : 1,
               "subunit" : "Grosz",
               "subunit_to_unit" : 100,
               "symbol" : "z\u0142",
               "symbol_first" : 0,
               "thousands_separator" : " "
            },
            "flag" : "\ud83c\uddf5\ud83c\uddf1",
            "geohash" : "u2vtturjveyqfmjgp1tb",
            "qibla" : 143.11,
            "roadinfo" : {
               "drive_on" : "right",
               "road" : "Wilcza",
               "speed_in" : "km/h"
            },
            "sun" : {
               "rise" : {
                  "apparent" : 1690946040,
                  "astronomical" : 1690936620,
                  "civil" : 1690943700,
                  "nautical" : 1690940640
               },
               "set" : {
                  "apparent" : 1691000640,
                  "astronomical" : 1691009940,
                  "civil" : 1691002980,
                  "nautical" : 1691005980
               }
            },
            "timezone" : {
               "name" : "Europe/Warsaw",
               "now_in_dst" : 1,
               "offset_sec" : 7200,
               "offset_string" : "+0200",
               "short_name" : "CEST"
            },
            "what3words" : {
               "words" : "exists.hotels.velocity"
            }
         },
         "bounds" : {
            "northeast" : {
               "lat" : 50.2101077,
               "lng" : 19.2473377
            },
            "southwest" : {
               "lat" : 50.2096946,
               "lng" : 19.246772
            }
         },
         "components" : {
            "ISO_3166-1_alpha-2" : "PL",
            "ISO_3166-1_alpha-3" : "POL",
            "_category" : "building",
            "_type" : "building",
            "city" : "Jaworzno",
            "city_district" : "\u015ar\u00f3dmie\u015bcie, Bory i Niedzieliska",
            "continent" : "Europe",
            "country" : "Poland",
            "country_code" : "pl",
            "house_number" : "50",
            "neighbourhood" : "Osiedle Pod\u0142\u0119\u017ce 1",
            "political_union" : "European Union",
            "postcode" : "43-600",
            "road" : "Wilcza",
            "suburb" : "Pod\u0142\u0119\u017ce"
         },
         "confidence" : 10,
         "formatted" : "Wilcza 50, 43-600 Jaworzno, Poland",
         "geometry" : {
            "lat" : 50.2099012,
            "lng" : 19.2469181
         }
      },
      {
         "annotations" : {
            "DMS" : {
               "lat" : "50\u00b0 12' 19.00800'' N",
               "lng" : "19\u00b0 16' 29.92800'' E"
            },
            "MGRS" : "34UCA7690362879",
            "Maidenhead" : "JO90pe29xg",
            "Mercator" : {
               "x" : 2145680.959,
               "y" : 6449053.155
            },
            "NUTS" : {
               "NUTS0" : {
                  "code" : "PL"
               },
               "NUTS1" : {
                  "code" : "PL2"
               },
               "NUTS2" : {
                  "code" : "PL22"
               },
               "NUTS3" : {
                  "code" : "PL22B"
               }
            },
            "OSM" : {
               "note_url" : "https://www.openstreetmap.org/note/new#map=16/50.20528/19.27498&layers=N",
               "url" : "https://www.openstreetmap.org/?mlat=50.20528&mlon=19.27498#map=16/50.20528/19.27498"
            },
            "UN_M49" : {
               "regions" : {
                  "EASTERN_EUROPE" : "151",
                  "EUROPE" : "150",
                  "PL" : "616",
                  "WORLD" : "001"
               },
               "statistical_groupings" : [
                  "MEDC"
               ]
            },
            "callingcode" : 48,
            "currency" : {
               "alternate_symbols" : [],
               "decimal_mark" : ",",
               "format" : "%n %u",
               "html_entity" : "z&#322;",
               "iso_code" : "PLN",
               "iso_numeric" : "985",
               "name" : "Polish Z\u0142oty",
               "smallest_denomination" : 1,
               "subunit" : "Grosz",
               "subunit_to_unit" : 100,
               "symbol" : "z\u0142",
               "symbol_first" : 0,
               "thousands_separator" : " "
            },
            "flag" : "\ud83c\uddf5\ud83c\uddf1",
            "geohash" : "u2vtweedgwdb9mseunpn",
            "qibla" : 143.15,
            "roadinfo" : {
               "drive_on" : "right",
               "speed_in" : "km/h"
            },
            "sun" : {
               "rise" : {
                  "apparent" : 1690946040,
                  "astronomical" : 1690936620,
                  "civil" : 1690943700,
                  "nautical" : 1690940640
               },
               "set" : {
                  "apparent" : 1691000640,
                  "astronomical" : 1691009940,
                  "civil" : 1691002920,
                  "nautical" : 1691005980
               }
            },
            "timezone" : {
               "name" : "Europe/Warsaw",
               "now_in_dst" : 1,
               "offset_sec" : 7200,
               "offset_string" : "+0200",
               "short_name" : "CEST"
            },
            "what3words" : {
               "words" : "middle.shopper.frocks"
            }
         },
         "bounds" : {
            "northeast" : {
               "lat" : 50.2580135,
               "lng" : 19.300041
            },
            "southwest" : {
               "lat" : 50.151742,
               "lng" : 19.16559
            }
         },
         "components" : {
            "ISO_3166-1_alpha-2" : "PL",
            "ISO_3166-1_alpha-3" : "POL",
            "_category" : "place",
            "_type" : "city",
            "continent" : "Europe",
            "country" : "Poland",
            "country_code" : "pl",
            "county" : "Jaworzno",
            "local_administrative_area" : "Jaworzno",
            "political_union" : "European Union",
            "state" : "Wojew\u00f3dztwo \u015al\u0105skie",
            "state_code" : "24",
            "town" : "Jaworzno"
         },
         "confidence" : 6,
         "formatted" : "Jaworzno, Poland",
         "geometry" : {
            "lat" : 50.20528,
            "lng" : 19.27498
         }
      }
   ],
   "status" : {
      "code" : 200,
      "message" : "OK"
   },
   "stay_informed" : {
      "blog" : "https://blog.opencagedata.com",
      "mastodon" : "https://en.osm.town/@opencage"
   },
   "thanks" : "For using an OpenCage API",
   "timestamp" : {
      "created_http" : "Wed, 02 Aug 2023 09:32:51 GMT",
      "created_unix" : 1690968771
   },
   "total_results" : 2
}
         */
        JSONArray results = ob.getJSONArray("results");
        if (results == null || results.isEmpty())
            return LatLon.EMPTY;

        // this is only a homework, so let's get only first result, we don't care that much
        JSONObject result = results.getJSONObject(0);
        if (result == null)
            return LatLon.EMPTY;

        JSONObject geometry = result.getJSONObject("geometry");
        if (geometry == null)
            return LatLon.EMPTY;

        String lat = Objects.toString(geometry.get("lat"), "0");
        String lon = Objects.toString(geometry.get("lng"), "0");
        return new LatLon(lat, lon);
    }

    private String getApiKey() {
        if (apiKey == null)
             apiKey = Objects.toString(System.getenv("OPENCAGE_API_KEY"), "");
        return apiKey;
    }
}
