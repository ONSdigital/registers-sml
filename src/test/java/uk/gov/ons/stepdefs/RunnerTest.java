package uk.gov.ons.stepdefs;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

// Set JUnit to run with Cucumber and configure both feature and step lookup locations.
@RunWith(Cucumber.class)
@CucumberOptions(plugin = "json:target/cucumber-report.json",
        features = "./resources/features",
        glue = "" /* only to be specified if in separate package */,
        tags = {"@George"})
public class RunnerTest {
    // Begin tests.

    @BeforeClass
    public static void setup() {
        //Create SparkSession
        if (Helpers.sparkSession == null) {
            System.out.println("CucumberJunitRunner BeforeClass : Created spark session.");
            Helpers.sparkSession = SparkSession
                    .builder()
                    .appName("BDD test app")
                    .master("local")
                    .getOrCreate();
            Helpers.sparkSession.sparkContext().setLogLevel("ERROR");
        }
    }

    @AfterClass
    public static void teardown() {
        //Close SparkSession
        if (Helpers.sparkSession != null) {
            Helpers.sparkSession.close();
            Helpers.sparkSession = null;
            System.out.println("CucumberJunitRunner AfterClass: After Closed spark session.");
        }
    }
}
