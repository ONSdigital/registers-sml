from behave import Given, When, Then
from python.sml.methods.duplicate import duplicate


@Given('the user provides the partition and order columns')
def step_impl(context):
    context.partition_cols = str(context.table.rows[0][0]).split(",")
    context.order_cols = str(context.table.rows[0][1]).split(",")


@When('the Python Duplicate Marker function is applied')
def step_impl(context):
    new_col = "Duplicate Marker"
    context.output_df = duplicate(context.input_data).dm1(context.input_data, context.partition_cols,
                                                        context.order_cols, new_col)
    print("Expected DataFrame ...")
    context.expected_data.select("id", "num", "order", "Duplicate Marker").show()
    print("Output DataFrame ...")
    context.output_df.select("id", "num", "order", "Duplicate Marker").show()


@Then('record with highest order column is marked with one to match expected data')
def step_impl(context):
    assert context.expected_data.select("id", "num", "order", "Duplicate Marker").collect() == \
                                              context.output_df.select("id", "num", "order",
                                                                       "Duplicate Marker").collect()