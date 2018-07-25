from behave import given, when, then


@given("an input JSON is at the location {input_JSON_ref}")
def step_given_input_json(context, input_JSON_ref):
    context.input_JSON_ref = input_JSON_ref


@given("an expected JSON is at the location {expected_JSON_ref}")
def step_given_expected_json(context, expected_JSON_ref):
    context.expected_JSON_ref = expected_JSON_ref


@given("the user specifies the uniquely identifying column(s) {list_of_IDs}")
def step_given_list_of_ids(context, list_of_IDs):
    context.list_of_IDs = list_of_IDs.split(",")


@given("the user specifies the unpivoting columns {list_of_columns}")
def step_given_list_of_columns(context, list_of_columns):
    context.list_of_columns = list_of_columns.split(",")


@given("the user names the new variable column {var_name}")
def step_given_var_name(context, var_name):
    context.var_name = var_name


@given("the user names the new value column {val_name}")
def step_given_val_name(context, val_name):
    context.val_name = val_name

@given("the user specifies the grouping columns {list_of_columns}")
def step_given_grouping_columns(context, list_of_columns):
    context.grouping_columns = list_of_columns.split(",")

@given("the user specifies the column to sum {sum_column}")
def step_given_sum_column(context, sum_column):
    context.sum_column = sum_column


@given("the user specifies the partition columns {part_cols}")
def step_partition_columns(context, part_cols):
    context.part_cols = part_cols.split(",")


@given("the user specifies the order columns {order_cols}")
def step_partition_columns(context, order_cols):
    context.order_cols = order_cols.split(",")


@given("the user specifies a new column name {new_col_name}")
def step_partition_columns(context, new_col_name):
    context.new_col_name = new_col_name

@given("the user specifies the target column {target_col}")
def step_partition_columns(context, target_col):
    context.target_col = target_col

@given("the user specifies the number to lag back on {lag_num:d}")
def step_partition_columns(context, lag_num):
    context.lag_num = lag_num




