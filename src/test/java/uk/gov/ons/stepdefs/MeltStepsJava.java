package uk.gov.ons.stepdefs;

import cucumber.api.java.en.When;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.convert.WrapAsJava$;
import uk.gov.ons.api.java.methods.MeltFactory;
import java.util.ArrayList;

public class MeltStepsJava {
    private Dataset<Row> inputDS = ContextMelt.input_data();
    @When("the Java Melt function is applied")
    public void java_melt_called() throws Exception {
        if (ContextMelt.var_name().contentEquals("null") || ContextMelt.value_name().contentEquals("null")) {
            System.out.print("Arguments cannot be defaulted in Java - Skip.\n\n");
            assert(1==1);
        }
        else {
            ContextCommon.output_data_$eq(MeltFactory.melt(inputDS)
                    .melt1(inputDS,
                            new ArrayList<>(WrapAsJava$.MODULE$.seqAsJavaList(ContextMelt.id_vars())),
                            new ArrayList<>(WrapAsJava$.MODULE$.seqAsJavaList(ContextMelt.value_vars())),
                            ContextMelt.var_name(), ContextMelt.value_name()));
            System.out.print("The Java Melt output\n");
            ContextCommon.output_data().show();
        }
    }
}
