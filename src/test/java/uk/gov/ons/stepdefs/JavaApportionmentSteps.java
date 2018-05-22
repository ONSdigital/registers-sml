package uk.gov.ons.stepdefs;

import com.sun.tools.javac.util.ListBuffer;
import cucumber.api.java.en.When;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.convert.WrapAsJava$;
import scala.collection.convert.WrapAsScala$;
import scala.collection.mutable.Buffer$;
import uk.gov.ons.api.java.methods.JavaApportionmentFactory;
import scala.collection.JavaConversions.*;

import java.nio.Buffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class JavaApportionmentSteps {

    @When("^the Java Apportionment function is applied to the dataset$")
    public void whenJavaApportionmentApplied() {
        // Load parameters, create DataFrame and apply Apportionment function with parameters
        Map<String, String> javaParamsMap = WrapAsJava$.MODULE$
                                       .mapAsJavaMap(ContextApportionment.paramsMap());
        Dataset<Row> inputDF = Helpers.sparkSession
                                      .read()
                                      .json(javaParamsMap.get("input_data"));

        ContextApportionment.resultDF_$eq(
                        JavaApportionmentFactory.apportionment(inputDF)
                                .app1(javaParamsMap.get("aux"),
                                    javaParamsMap.get("date"),
                                    javaParamsMap.get("aggregate A"),
                                    javaParamsMap.get("aggregate B"),
                                    new ArrayList<>(Arrays.asList(javaParamsMap.get("apportion").split(","))))
        );

        System.out.println("When the Java Apportionment function is applied to the dataset");
    }
}
