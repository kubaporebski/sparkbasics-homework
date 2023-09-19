package jporebski.data.sparkbasics;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class AzureUploadTest {

    @Test
    public void testUpload() throws Exception {
/*
        SparkConf config = new SparkConf();
        config.setAppName("sparkbasics");
        config.setMaster("local[*]");
        config.set("fs.azure.account.auth.type.jakubporebskistorage.dfs.core.windows.net", "OAuth");
        config.set("fs.azure.account.oauth.provider.type.jakubporebskistorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
        config.set("fs.azure.account.oauth2.client.id.jakubporebskistorage.dfs.core.windows.net", "sparkbasics-app");
        config.set("fs.azure.account.oauth2.client.secret.jakubporebskistorage.dfs.core.windows.net", service_credential);
        config.set("fs.azure.account.oauth2.client.endpoint.jakubporebskistorage.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token");
        SparkContext context = new SparkContext(config);

        try (SparkSession sparkSession = new SparkSession(context)) {
            MapFunction<Long, SimpleRow> mapFn = val -> new SimpleRow(val, Math.random() * val);

            sparkSession
                    .range(-10, 10)
                    .map(mapFn, Encoders.bean(SimpleRow.class))
                    .write()
                    .mode("overwrite")
                    .parquet("abfss://jpblobcontainer@jakubporebskistorage.dfs.core.windows.net/simple-rows");
        }*/
    }

    static class SimpleRow {

        public long X;

        public double Y;

        public SimpleRow() { }

        public SimpleRow(long x, double y) {
            this.X = x;
            this.Y = y;
        }

        public long getX() {
            return X;
        }

        public void setX(long x) {
            X = x;
        }

        public double getY() {
            return Y;
        }

        public void setY(double y) {
            Y = y;
        }
    }

}
