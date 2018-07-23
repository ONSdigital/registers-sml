from behave import Given
def df_from_file(context, filepath):
    if filepath.endswith("csv"):
        df = context.spark.read.option("header", "true").csv(filepath)
    elif filepath.endswith("json"):
        df = context.spark.read.json(filepath)
    else:
        raise Exception("File must be in CSV or JSON format")
    return df


@Given("the user provides the file paths")
def given_paths(context):
    context.input_data = df_from_file(context, str(context.table.rows[0][0]))
    context.expected_data = df_from_file(context, str(context.table.rows[0][1]))

@Given('the user provides the function parameters')
def step_impl(context):
    context.parameters = context.table.rows[0]