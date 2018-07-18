package uk.gov.ons.api.java.methods;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import uk.gov.ons.methods.FirstReturn;

/**
 *This class is a factory to create FirstReturnAPI instance.
 */
public class FirstReturnFactory {
    private static final FirstReturnAPI$ FIRST_RETURN_API = FirstReturnAPI$.MODULE$;

    private FirstReturnFactory() {
    }

    /**
     *
     * @param df
     * @return
     */
    public static FirstReturnAPI firstReturn(Dataset<Row> df) {
        return FIRST_RETURN_API.firstReturn(df);
    }
}
