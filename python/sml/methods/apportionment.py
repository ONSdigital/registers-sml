from pyspark.sql import DataFrame


class Apportionment:

    def __init__(self, df=None):

        if df is None:
            raise TypeError

        self._df = df

        self._jApp = self._df._sc._jvm                                          \
                         .uk.gov.ons                                            \
                         .api.java.methods                                      \
                         .JavaApportionmentFactory.apportionment(self._df._jdf)

    def __mandatory_argument_check(self, *args):
        for arg in args:
            if arg is None:
                raise TypeError

    def app1(self, aux_col=None, date_col=None,
             agg_col_a=None, agg_col_b=None, app_cols=None):

        """
        A Python wrapper for the Apportionment function.

        :param aux_col: Column used as a weighting.
        :param date_col: Column used to differentiate records of the same reference.
        :param agg_col_a: Column containing initial reference.
        :param agg_col_b: Column containing reference to apportion to.
        :param app_cols: Column(s) to be redistributed.
        :return: pyspark.sql.DataFrame
        """

        self.__mandatory_argument_check(aux_col, date_col, agg_col_a, agg_col_b, app_cols)

        df = self._df

        return DataFrame(self._jApp.app1(aux_col, date_col,
                                         agg_col_a, agg_col_b, app_cols),
                         df.sql_ctx)


def apportionment(df):

    return Apportionment(df)
