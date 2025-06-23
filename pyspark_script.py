from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dag_job").getOrCreate()
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)
df.show()
print("✅ PySpark Job Completed Successfully!")
spark.stop()
