package uk.gov.ons.stepdefs;

import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.convert.WrapAsJava$;
import uk.gov.ons.api.java.methods.MarkingBetweenLimitsFactory;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class MarkingBetweenLimitsJava {
    Dataset<Row> outDf;

    // Given steps in common steps file
    // Common function to calling the markingBetweenLimitsMethod
    public void calling_the_method(Dataset<Row> inputDf) throws Exception {
        ArrayList<String> parameters = new ArrayList<>(WrapAsJava$.MODULE$.seqAsJavaList(ContextCommon.param_list()));
        ArrayList<String> partCol = new ArrayList(Arrays.asList(parameters.get(3).split(",")));
        ArrayList<String> ordCol = new ArrayList(Arrays.asList(parameters.get(4).split(",")));
        outDf = MarkingBetweenLimitsFactory.markingBetweenLimits(inputDf).markingBetweenLimits(inputDf,
                parameters.get(0), parameters.get(1), parameters.get(2), partCol, ordCol, parameters.get(5));

    }

    // This then step is used across all java scenarios.
    @Then("^the Java record will be flagged as (\\d+)$")
    public void then_flagged(Integer flagValue) throws Exception {
        Dataset<Row> expectedDf = ContextCommon.expected_data();
        assertEquals(expectedDf.select("id", "date", "value",
                "marker").orderBy("id", "date").collectAsList(),
                outDf.select("id", "date", "value",
                        "marker").orderBy("id", "date").collectAsList());
    }

    //  Scenario : Ratio of Current and Previous value falls inbetween Limits
    @When("^the Java value divided by the previous value falls in between the prescribed upper and lower values$")
    public void when_value_between() throws Exception {
        Dataset<Row> inputDf = ContextCommon.input_data();
        calling_the_method(inputDf);
    }

    // Scenario : Ratio of Current and Previous value is outside of Limits
    @When("^the Java value divided by its previous is greater than the upper value or less than the lower limit$")
    public void when_outside_limits() throws Exception {
        Dataset<Row> inputDf = ContextCommon.input_data();
        calling_the_method(inputDf);
    }

    // Scenario : No previous value
    @When("^the Java current value is divided by the previous value, where previous value is null$")
    public void when_no_previous() throws Exception {
        Dataset<Row> inputDf = ContextCommon.input_data();
        calling_the_method(inputDf);
    }


    // Scenario : No current value
    @When("^there is no current value to perform the Java calculation$")
    public void when_no_current() throws Exception {
        Dataset<Row> inputDf = ContextCommon.input_data();
        calling_the_method(inputDf);
    }

    // Scenario : Previous value is zero
    @When("^the Java current value is divided by the previous value, where previous value is zero$")
    public void when_previous_zero() throws Exception {
        Dataset<Row> inputDf = ContextCommon.input_data();
        calling_the_method(inputDf);
    }
}
