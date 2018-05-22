from behave import Given, When, Then
from python.sml.methods.apportionment import apportionment


# Given("""^there are datasets at the locations:$""")

    # Do common step


# Given("""^the user provides the parameters:$""")

    # Do common step


@Given("there is a simple relationship between two partitions")
def given_simple_relationship(context):

    # Passing step, functionality tested by feature examples
    pass


@Given("there is a complex relationship between two partitions")
def given_complex_relationship(context):

    # Passing step, functionality tested by feature examples
    pass


@When("the Python Apportionment function is applied to the dataset")
def when_apportioning(context):

    # Load parameters, create DataFrame and apply Apportionment function with parameters
    context.params = context.data_row + context.param_row

    input_df = context.spark                    \
                      .read                     \
                      .json(context.params[0])

    context.result_df = apportionment(input_df).app1(context.params[1],
                                                     context.params[2],
                                                     context.params[3],
                                                     context.params[4],
                                                     context.params[5].split(","))


@Then("the Apportionment function will apportion the value to the output, matching an expected dataset at a location {expected_path}")
def then_assert_df_equivalence(context, expected_path):

    # Assert that the output DataFrame matches a DataFrame created from the expected data
    expected_df = context.spark                 \
                         .read                  \
                         .json(expected_path)

    # Pull out columns from expected DataFrame to re-order columns in output DataFrame
    select_list = expected_df.columns

    assert context.result_df                        \
                  .select(select_list)              \
                  .orderBy("vatref9", "turnover")   \
                  .collect()                        \
          ==                                        \
          expected_df.select(select_list)           \
                     .orderBy("vatref9", "turnover")\
                     .collect()

@Then("the Apportionment function ensures that no turnover is lost in the output")
def then_assert_no_turnover_loss(context):

    # Assert that turnover is not lost for each aggregate B reference
    target0 = context.params[5].split(",")[0]
    agg_b = context.params[4]

    for reference in context.result_df.select(agg_b).distinct().collect()[0]:
        assert context.result_df                        \
                      .filter(agg_b + "=" + reference)  \
                      .groupBy(agg_b)                   \
                      .sum(target0)                     \
                      .select("sum(" + target0 + ")")   \
                      .first()                          \
               ==                                       \
               context.result_df                        \
                      .filter(agg_b + "=" + reference)  \
                      .select(target0 + "_apportioned") \
                      .first()
