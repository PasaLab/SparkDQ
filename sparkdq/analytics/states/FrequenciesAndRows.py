from pyspark.sql.functions import coalesce, col, expr, lit

from sparkdq.analytics.CommonFunctions import COUNT_COL
from sparkdq.analytics.states.State import State


class FrequenciesAndRows(State):

    def __init__(self, frequencies, num_rows):
        self.frequencies = frequencies
        self.num_rows = num_rows

    # def sum(self, other):
    #     count_col = COUNT_COL
    #     columns = self.frequencies.schema.names
    #     columns.remove(count_col)
    #     add_after_join = [coalesce(col("this.{}".format(c)), col("other.{}".format(c))).alias(c) for c in columns]
    #     add_after_join.append((coalesce(col("this.{}".format(count_col)), lit(0)) +
    #                            coalesce(col("other.{}".format(count_col)), lit(0))).alias(count_col))
    #     join_condition = expr(str(True))
    #     for c in columns:
    #         join_condition = join_condition & (col("this.{}".format(c)) == col("other.{}".format(c)))
    #     frequencies_sum = self.frequencies.alias("this")\
    #         .join(other.frequencies.alias("other"), join_condition, "outer")\
    #         .select(*add_after_join)
    #     return FrequenciesAndRows(frequencies_sum, self.num_rows + other.num_rows)




