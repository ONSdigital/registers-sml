from pyspark.sql import DataFrame


class Duplicate():
    """
    This class contains a methods which mark duplicate records on a DataFrame
    """

    defaultCol = "DuplicateMarking"

    def __init__(self, df=None):
        """
        Initialise function to instantiate the Duplicate class

        :param df: a DataFrame
        """

        if df is None:
            raise TypeError

        self._df = df

        self._jDup = self._df._sc._jvm.uk.gov.ons.api.java.methods.JavaDuplicateFactory.duplicate(self._df._jdf)

    def __mandatoryArgumentCheck(self, arg1, arg2, arg3):
        if (arg1 is None) or (arg2 is None) or (arg3 is None):
            raise TypeError

    def dm1(self, df=None, partCol=None, ordCol=None, new_col=None):
        """
        Version 1 of the Duplicate Function

        :author: Ian Edward
        :param df:          - a DataFrame
        :param partCol:     - Column(s) to partition on
        :param ordCol:      - Column(s) to order on
        :param new_col:     - New column name
        :return:            - pyspark.sql.DataFrame
        """

        self.__mandatoryArgumentCheck(partCol, ordCol, new_col)

        if df is None:
            df = self._df

        return DataFrame(
            self._jDup.dm1(
                df._jdf,
                partCol,
                ordCol,
                new_col
            ),
            df.sql_ctx
        )


def duplicate(df):
    return Duplicate(df)