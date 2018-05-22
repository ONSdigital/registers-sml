from behave import fixture, use_fixture
from pyspark.sql import SparkSession


@fixture()
def create_session(context):
    context.spark = SparkSession.builder                                                                    \
                                .master("local")                                                            \
                                .appName("Behave")                                                          \
                                .config("spark.jars", "target/registers-sml-1.0-SNAPSHOT-jar-with-dependencies.jar")  \
                                .getOrCreate()
    context.spark.sparkContext.setLogLevel("ERROR")


@fixture()
def kill_session(context):
    context.spark.stop()


def before_all(context):
    use_fixture(create_session, context)


def after_all(context):
    use_fixture(kill_session, context)
