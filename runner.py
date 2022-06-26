import os
from typing import NoReturn

import pyspark.sql.dataframe
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType


class PysparkRunner:
    def __init__(self):
        self.spark_session = SparkSession.builder \
            .master("local") \
            .appName("signals") \
            .getOrCreate()
        self.spark_session.conf.set(
            "mapreduce.fileoutputcommitter.marksuccessfuljobs",
            "false")
        self.input_file = self._read_file()
        self.output_file = self._calculate()

    def _read_file(self) -> pyspark.sql.dataframe.DataFrame:
        df = self.spark_session.read.parquet(os.environ.get('INPUT_PATH'))
        return df

    def _calculate(self) -> pyspark.sql.dataframe.DataFrame:
        w1 = Window.partitionBy("entity_id").orderBy(col("month_id"),
                                                     col("item_id").desc())
        w2 = Window.partitionBy("entity_id").orderBy(col("month_id"),
                                                     col("item_id"))
        w3 = Window.partitionBy("entity_id")

        result = self.input_file \
            .withColumn("row_num_formax", F.row_number().over(w1)) \
            .withColumn("row_num_formin", F.row_number().over(w2)) \
            .withColumn("max_row_num", F.max("row_num_formax").over(w3)) \
            .withColumn("min_row_num", F.min("row_num_formin").over(w3)) \
            .withColumn("max_item_id",
                        F.when(col("row_num_formax") == col("max_row_num"),
                               col("item_id")).otherwise(F.lit(None))) \
            .withColumn("min_item_id",
                        F.when(col("row_num_formin") == col("min_row_num"),
                               col("item_id")).otherwise(F.lit(None))) \
            .withColumn("total_signals", F.sum(col("signal_count")).over(w2)) \
            .groupBy("entity_id").agg(F.max(col("min_item_id")),
                                      F.max(col("max_item_id")),
                                      F.max(col("total_signals"))) \
            .withColumnRenamed("max(min_item_id)", "oldest_item_id") \
            .withColumnRenamed("max(max_item_id)", "newest_item_id") \
            .withColumnRenamed("max(total_signals)", "total_signals") \
            .withColumn("total_signals",
                        col("total_signals").cast(IntegerType()))
        return result

    def save_output(self):
        self.output_file.coalesce(1).write.mode("overwrite").parquet(
            os.environ.get('OUTPUT_PATH'))


def check_paths() -> NoReturn:
    out_path = os.environ.get('OUTPUT_PATH')
    in_path = os.environ.get('INPUT_PATH')
    if not out_path:
        out_path = in_path + '_result'
        os.environ.update({'OUTPUT_PATH': str(out_path)})


if __name__ == '__main__':
    check_paths()
    ex = PysparkRunner()
    ex.save_output()
