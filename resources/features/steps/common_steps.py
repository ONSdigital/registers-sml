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

@Given("there are datasets at the locations")
def given_datasets(context):

    # Load data table into context
    context.data_row = []

    for x in context.table.rows[0]:
        context.data_row.append(str(x))


@Given("the user provides the parameters")
def given_parameters(context):

    # Load data table into context
    context.param_row = []

    for x in context.table.rows[0]:
        context.param_row.append(str(x))