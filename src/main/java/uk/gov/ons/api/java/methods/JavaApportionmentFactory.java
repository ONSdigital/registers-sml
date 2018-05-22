package uk.gov.ons.api.java.methods;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaApportionmentFactory {

    private static final JavaApportionment$ JAVA_APPORTIONMENT_$ = JavaApportionment$.MODULE$;

    private JavaApportionmentFactory() {}

    /**
     * A Java wrapper for the Apportionment function.
     *
     * @param df    Dataset[Row] - dataset to transform.
     * @return      Dataset[Row]
     */
    public static JavaApportionment apportionment(Dataset<Row> df) {

        return JAVA_APPORTIONMENT_$.apportionment(df);
    }
}
