from pyspark.sql import DataFrame


class MarkingBetweenLimits():
    def __init__(self, df=None):
        if df is None: raise TypeError

        self._df = df
        self._jmbl = self._df._sc._jvm.uk.gov.ons.api.java.methods.MarkingBetweenLimitsFactory.markingBetweenLimits(
            self._df._jdf)

    def __mandatoryArgumentCheck(self, *args):
        for arg in args:
            if arg is None:
                raise TypeError

    def markingBetweenLimits(self, df=None, valueColumnName=None, upperLimit=None, lowerLimit=None,
                             partitionColumns=None, orderColumns=None, newColumnName=None):
        """
        A Python wrapper for the markingBetweenLimits function

        :param df: a DataFrame
        :param valueColumnName: Name of the value column to mark
        :param upperLimit: The upper limit to compare to
        :param lowerLimit: The lower limit to compare to
        :param partitionColumns: The columns in which to partition on when getting the previous value
        :param orderColumns: The columns in which to order by when getting the previous value
        :param newColumnName: Name of the new Column to make
        :return: pyspark.sql.DataFrame
        """
        self.__mandatoryArgumentCheck(valueColumnName, upperLimit, lowerLimit, partitionColumns, orderColumns,
                                      newColumnName)
        if df is None: df = self._df
        return DataFrame(
            self._jmbl.markingBetweenLimits(df._jdf, valueColumnName, upperLimit, lowerLimit, partitionColumns,
                                            orderColumns, newColumnName), df.sql_ctx)


def markingBetweenLimits(df):
    return MarkingBetweenLimits(df)
