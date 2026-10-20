import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestSpark").master("local[1]").getOrCreate()
print(f"Spark Version: {spark.version}")
df = spark.createDataFrame([(1, "AA"), (2, "DL")], ["id", "carrier"])
df.show()
spark.stop()
