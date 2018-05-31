package uk.gov.ons.api.java.methods;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class MeltFactory {

    private static final MeltAPI$ MELT_API = MeltAPI$.MODULE$;

    private MeltFactory() {
    }

    /**
     * Factory class which initialises the MeltAPI object for use
     *
     * @param df - Input DataFrame
     * @return df
     */
    public static MeltAPI melt(Dataset<Row> df) {
        return MELT_API.melt(df);
    }

}
