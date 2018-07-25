package uk.gov.ons.api.java.methods;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public final class JavaDuplicateFactory {

    private static final JavaDuplicate$ JAVA_DUPLICATE = JavaDuplicate$.MODULE$;

    private JavaDuplicateFactory() {}

    /**
     *
     * Factory Class which initialises the DuplicateAPI object for use
     *
     * @param df - Input DataFrame
     * @return
     *
     */

    public static JavaDuplicate duplicate(Dataset<Row> df){
        return JAVA_DUPLICATE.duplicate(df);}
}
