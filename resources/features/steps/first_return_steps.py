from behave import Given, When, Then
from pyspark.sql import functions as F
import python.sml.methods.FirstReturn as FR
from pyspark.sql.types import DoubleType

@Given(
    u'the user provides column names for the parameters partition,order,new column,which items first return and threshold percentage')
def step_user_passed_parameter(context):
    context.partition_cols = str(context.table.rows[0][0]).split(",")
    context.order_cols = str(context.table.rows[0][1]).split(",")
    context.new_col_name = str(context.table.rows[0][2])
    context.which_item_fr = str(context.table.rows[0][3])
    context.threshold_percentage = float(context.table.rows[0][4])


@When(u'we apply the Python first returns function to identify first return and large first return')
def step_when_first_return_called(context):
    context.fr_actual_df = FR.firstReturn(context.input_data) \
        .firstReturn1(context.input_data, context.partition_cols,
                      context.order_cols, context.new_col_name
                      , context.threshold_percentage, context.which_item_fr) \
        .select("id", "period", "turnover", "turnover_fr_flag") \
        .orderBy("period", "id", "turnover_fr_flag")
    print("Python First_Return :: Actual outcome ::")
    context.fr_actual_df.show(70)


@Then(
    u'the Python first return should be marked as "1",large first returns as "2",otherwise "0" as in data from the file location expected_data_path')
def step_check_first_return_actual_expected_outcome(context):
    print("Python First_Return :: Actual outcome ::")
    context.expected_data.show(70)
    assert context.fr_actual_df.collect() == context.expected_data.select(F.col("id"), F.col("period"), F.col("turnover"),
                                                                        F.col("turnover_fr_flag").cast(
                                                                            DoubleType())).orderBy("period", "id",
                                                                                                  "turnover_fr_flag").collect()
