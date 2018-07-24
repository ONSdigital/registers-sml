from pyspark.sql import DataFrame


class Melt:
    """
    This class contains methods which perform a melt (the reverse of a pivot) on a DataFrame.
    """

    def __init__(self, df=None):
        """
        Initialise function to instantiate the Melt class.

        :param df: a DataFrame
        """

        if df is None:
            raise TypeError

        self._df = df

        self._jMelt = self._df._sc._jvm.uk.gov.ons.api.java.methods.MeltFactory.melt(self._df._jdf)

    def melt1(self, df=None, id_vars=None, value_vars=None, var_name="variable", val_name="value"):
        """
        Version 1 of the melt function.

        :param df: a DataFrame
        :param id_vars: Column(s) which are used as unique identifiers
        :param value_vars: Column(s) which are being unpivoted
        :param var_name: The name of a new column, which holds all the value_vars names, defaulted to
                         variable.
        :param val_name: The name of a new column, which holds all the values of value_vars column(s),
                         defaulted to value.
        :return: pyspark.sql.DataFrame
        """

        if df is None:
            df = self._df

        return DataFrame(self._jMelt.melt1(df._jdf, id_vars, value_vars, var_name, val_name), df.sql_ctx)


def melt(df):
    return Melt(df)
