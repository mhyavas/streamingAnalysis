

#Reading from DB and grouped by Start Time
from pyspark.sql import SparkSession
import time

from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("DBgrouping").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setCheckpointDir("checkpoint/dbgrouping/dbgroupCP")
while True:

    postgre = spark.read.format("jdbc") \
        .option("url","jdbc:postgresql://127.0.0.1:5432/aggtrade") \
        .option("dbtable", "adausdt_test") \
        .option("user","postgres") \
        .option("password","123456").load()

    groupedDF = postgre.select("start_time", "end_time", "key", "sum_qty", "avg_price", "max_price", "min_price")\
        .groupBy("start_time","end_time","key")\
        .agg(round(sum(postgre.sum_qty),3).alias("sum_qty"),round(avg(postgre.avg_price),3).alias("avg_price"),
             round(max(postgre.max_price),3).alias("max_price"), round(min(postgre.min_price),3).alias("min_price"))
    groupedDF.write.jdbc(url="jdbc:postgresql://127.0.0.1:5432/aggtrade", table="dotusdt_test", mode="overwrite",
                         properties={"user": "postgres", "password":"123456"})
    groupedDF.show()
    groupedDF.checkpoint()
    time.sleep(300)