from behave import given, then, when

from python.sml.methods.melt import Melt


@given("the user provides the melt function parameters")
def given_melt_parameters(context):
    context.melt_id_vars = str(context.table.rows[0][0]).split(",")
    context.melt_value_vars = str(context.table.rows[0][1]).split(",")
    context.melt_var_name = str(context.table.rows[0][2])
    context.melt_value_name = str(context.table.rows[0][3])


@when("the Python Melt function is applied")
def apply_melt_function(context):
    if context.melt_var_name != "null" and context.melt_value_name != "null":

        context.melt_output_data = Melt(context.input_data).melt1(id_vars=context.melt_id_vars,
                                                                  value_vars=context.melt_value_vars,
                                                                  var_name=context.melt_var_name,
                                                                  val_name=context.melt_value_name)
    elif context.melt_var_name == "null" and context.melt_value_name == "null":
        context.melt_output_data = Melt(context.input_data).melt1(id_vars=context.melt_id_vars,
                                                                  value_vars=context.melt_value_vars)
    elif context.melt_var_name != "null" and context.melt_value_name == "null":
        print(context.melt_id_vars)
        print(context.melt_value_vars)
        context.melt_output_data = Melt(context.input_data).melt1(id_vars=context.melt_id_vars,
                                                                  value_vars=context.melt_value_vars,
                                                                  var_name=context.melt_var_name)
    elif context.melt_var_name == "null" and context.melt_value_name != "null":
        context.melt_output_data = Melt(context.input_data).melt1(id_vars=context.melt_id_vars,
                                                                  value_vars=context.melt_value_vars,
                                                                  val_name=context.melt_value_name)
    print("Input Data")
    context.input_data.show()
    context.input_data.printSchema()
    print("Python Melt output")
    context.melt_output_data.show()
    context.melt_output_data.printSchema()
    print("Expected Output")
    context.expected_data.show()
    context.expected_data.printSchema()


@then("the DataFrame will be unpivoted correctly")
def unpivoted_correctly(context):
    if context.melt_value_name == "null":
        context.melt_value_name = "value"
    if context.melt_var_name == "null":
        context.melt_var_name = "variable"

    assert (context.melt_output_data.select("identifier", "date",
                                            context.melt_var_name, context.melt_value_name)
            .orderBy("identifier", "date", context.melt_value_name, context.melt_var_name).collect()
            == context.expected_data.select("identifier", "date",
                                            context.melt_var_name, context.melt_value_name)
            .orderBy("identifier", "date", context.melt_value_name, context.melt_var_name).collect())
