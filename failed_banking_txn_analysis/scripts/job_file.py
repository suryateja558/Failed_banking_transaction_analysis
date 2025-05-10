from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CleanMergeAndFilterTransactions").getOrCreate()

df = spark.read.option("header", "true").csv("gs://new-project1-bucket/transactions/*.csv")

df_clean = df.na.drop(how="any").filter((col("transaction_id") != "") & (col("status") != ""))

failed_txns = df_clean.filter(col("status") == "FAILED")

jdbc_url="jdbc:mysql://35.226.72.209:3306/new_database"

properties={
    "user":"myself",
    "password":"Surya@558",
    "driver":"com.mysql.cj.jdbc.Driver"
}

failed_txns.write.jdbc(url=jdbc_url, table="failed_transactions", mode="overwrite", properties=properties)

spark.stop()
