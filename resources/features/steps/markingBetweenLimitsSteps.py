from behave import When, Then

from python.sml.methods.markingBetweenLimits import markingBetweenLimits


def calling_method(context, inputDf):
    part_cols = str(context.parameters[3]).split(",")
    order_cols = str(context.parameters[4]).split(",")
    mbl_class = markingBetweenLimits(inputDf)
    result_df = mbl_class.markingBetweenLimits(inputDf, str(context.parameters[0]), str(context.parameters[1]),
                                               str(context.parameters[2]), part_cols, order_cols,
                                               str(context.parameters[5]))
    return result_df


# Scenario: Ratio of Current and Previous value falls inbetween Limits
@When('the Python value divided by the previous value falls in between the prescribed upper and lower values')
def step_impl(context):
    input_df = context.input_data
    context.result_df = calling_method(context, input_df)


@Then('the Python record will be flagged as {flag_value}')
def step_impl(context, flag_value):
    expected_df = context.expected_data
    assert context.result_df.select("id", "date", "value", "marker").orderBy("id", "date").collect() \
           == expected_df.select("id", "date", "value", "marker").orderBy("id", "date").collect()


# Scenario: Ratio of Current and Previous value is outside of Limits
@When('the Python value divided by its previous is greater than the upper value or less than the lower limit')
def step_impl(context):
    input_df = context.input_data
    context.result_df = calling_method(context, input_df)


# Scenario: No previous value
@When('the Python current value is divided by the previous value, where previous value is null')
def step_impl(context):
    input_df = context.input_data
    context.result_df = calling_method(context, input_df)


# Scenario: No current value
@When('there is no current value to perform the Python calculation')
def step_impl(context):
    input_df = context.input_data
    context.result_df = calling_method(context, input_df)


# Scenario: Previous value is zero
@When('the Python current value is divided by the previous value, where previous value is zero')
def step_impl(context):
    input_df = context.input_data
    context.result_df = calling_method(context, input_df)
