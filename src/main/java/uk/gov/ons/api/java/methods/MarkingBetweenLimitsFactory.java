package uk.gov.ons.api.java.methods;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class MarkingBetweenLimitsFactory {

    private static final MarkingBetweenLimitsAPI$ MarkingBetweenLimits_API = MarkingBetweenLimitsAPI$.MODULE$;

    private MarkingBetweenLimitsFactory() {
    }

    /**
     * This Factory class with intialise an MarkingBetweenLimitsAPI object for use.
     *
     * @param df DataFrame
     * @return df DataFrame
     */
    public static MarkingBetweenLimitsAPI markingBetweenLimits(Dataset<Row> df) {
        return MarkingBetweenLimits_API.markingBetweenLimits(df);
    }
}
