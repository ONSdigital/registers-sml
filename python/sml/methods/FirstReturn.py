from pyspark.sql import DataFrame


class FirstReturn():
    def __init__(self, df=None):
        """
        Instantiates a FirstReturnAPI from a DataFrame.
        :param df: pyspark DataFrame
        """
        if df is None:
            raise TypeError
        self._df = df
        self._jFirstRtn = self._df._sc._jvm.uk.gov.ons.api.java.methods.FirstReturnFactory.firstReturn(self._df._jdf)

    def __mandatoryArgumentCheck(self, arg1, arg2, arg3, arg4, arg5):
        """
        This function raise Type Error if any one of the input arg is None.
        :param arg1:
        :param arg2:
        :param arg3:
        :param arg4:
        :param arg5:
        :return:
        """
        if (arg1 is None) | (arg2 is None) | (arg3 is None) | (arg4 is None) | (arg5 is None):
            raise TypeError('Missing mandatory argument')

    def firstReturn1(self, df, partitionColumns, orderColumns, newColName, thresholdPercentage, whichItemFr):
        """
        This function add an extra column to flag up the first occurrence and also large first occurrence.
        :param df: pyspark DataFrame
        :param partitionColumns: A list of Strings - column(s) names.
        :param orderColumns: A list of Strings - column(s) names.
        :param newColName: A string - new column name
        :param thresholdPercentage: A double - The first occurrence above this threshold should be flagged.
        :param whichItemFr: A string - which item first return have to be marked for example here it is turnover
        :return: pyspark DataFrame with a newly added marker column
        """
        self.__mandatoryArgumentCheck(partitionColumns, orderColumns, newColName, thresholdPercentage, whichItemFr)
        if df is None:
            df = self._df
        return DataFrame(self._jFirstRtn.firstReturn1(df._jdf, partitionColumns
                                                      , orderColumns, newColName, thresholdPercentage
                                                      , whichItemFr), df.sql_ctx)


def firstReturn(df):
    """

    :param df:
    :return:
    """
    return FirstReturn(df)
