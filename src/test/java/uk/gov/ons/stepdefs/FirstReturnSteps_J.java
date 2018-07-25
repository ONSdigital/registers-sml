package uk.gov.ons.stepdefs;

import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.convert.WrapAsJava$;
import uk.gov.ons.api.java.methods.FirstReturnAPI;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

//import scala.collection.JavaConversions._

public class FirstReturnSteps_J {
    Dataset<Row> firstReturnDs = null;
    @When("^we apply the Java first returns function to identify first return and large first return$")
    public void apply_java_first_return() throws Exception {
    ArrayList<String> partCol = new ArrayList<>(WrapAsJava$.MODULE$.seqAsJavaList(FirstReturnContext.partition_list()));
    ArrayList<String> ordCol = new ArrayList<>(WrapAsJava$.MODULE$.seqAsJavaList(FirstReturnContext.order_list()));

        firstReturnDs = FirstReturnAPI.firstReturn(ContextCommon.input_data())
            .firstReturn1(ContextCommon.input_data(),partCol
                        ,ordCol,FirstReturnContext.new_cols_name()
                        ,FirstReturnContext.threshold_percentage(),FirstReturnContext.which_item_fr());
        System.out.println("Java First return:: Actual outcome :: ");
        firstReturnDs.select("id", "period", "turnover", "turnover_fr_flag").orderBy("period", "id", "turnover_fr_flag").show(70);
    // Write code here that turns the phrase above into concrete actions
    //throw new PendingException();
}
    @Then("^the Java first return should be marked as \"([^\"]*)\",large first returns as \"([^\"]*)\",otherwise \"([^\"]*)\" as in data from the file location expected_data_path$")
    public void check_java_first_return_actual_outcome_expected_outcome(String arg1, String arg2, String arg3) throws Exception {
        System.out.println("Java First return:: Expected outcome :: ");
        ContextCommon.expected_data().show(70);
        assertEquals(ContextCommon.expected_data().orderBy("period", "id", "turnover_fr_flag").collectAsList()
                , firstReturnDs.select("id", "period", "turnover", "turnover_fr_flag").orderBy("period", "id", "turnover_fr_flag").collectAsList());
    }
}
