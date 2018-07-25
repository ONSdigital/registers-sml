package uk.gov.ons.stepdefs;
import cucumber.api.java.en.When;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.convert.WrapAsJava$;
import uk.gov.ons.api.java.methods.JavaDuplicate;
import uk.gov.ons.api.java.methods.JavaDuplicateFactory;

import java.util.ArrayList;

public class DuplicateMarkerJavaSteps {
    Dataset<Row> outDF;


    @When("^the Java Duplicate Marker function is applied$")
    public void java_duplicate_called() throws Exception {
        JavaDuplicate Transform = JavaDuplicateFactory.duplicate(ContextCommon.input_data());
        ArrayList<String> partCol = new ArrayList<>(WrapAsJava$.MODULE$.seqAsJavaList(ContextDuplicate.partition_list()));
        ArrayList<String> ordCol = new ArrayList<>(WrapAsJava$.MODULE$.seqAsJavaList(ContextDuplicate.order_list()));
        outDF = Transform.dm1(ContextCommon.input_data(), partCol, ordCol, DuplicateContext.new_column_name());
        System.out.println("The Java Implementation");
        System.out.println("Java Duplicate DataFrame");
        outDF.show();
    }
}
