import lit as lit
from lit.ShCommands import Seq
from pyspark import SparkContext
from pyspark.sql import SQLContext, DataFrameWriter
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import pyspark
import time

schema = StructType([
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("time", TimestampType(), True),
])
spark = SparkSession.builder.master("local").appName("denemeSession").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "aggTrade") \
    .option("startingOffsets", "latest") \
    .load()
df_string = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp as STRING)")

values = df_string.select("key", get_json_object(df_string.value, '$.price').alias("price"),
                          get_json_object(df_string.value, '$.quantity').alias("quantity"),
                          (get_json_object(df_string.value, '$.time').cast("long")/1000).cast("timestamp").alias('time'),
                          to_timestamp("timestamp").alias("kafka"))

stream1 = values.select("key", "price", "quantity", "time").filter(col("key") == "dotusdt").groupBy(
    window("time", "5 minutes", "1 minutes"), "key") \
    .agg(round(sum(values.quantity), 3).alias("sum_qty"), round(avg(values.price), 3).alias("avg_price"),
         round(max(values.price), 3).alias("max_price"), round(min(values.price), 3).alias("min_price"))

stream1_2 = stream1.select(stream1.window.start.cast(TimestampType()).alias("start_time"),
                           stream1.window.end.cast(TimestampType()).alias("end_time"), "key", "sum_qty", "avg_price",
                           col("max_price").cast("float"), col("min_price").cast("float"))
#stream1_2.writeStream.format("console").outputMode("complete").start().awaitTermination()

stream2 = values.select("key", "price", "quantity", "time").filter(col("key") == "adausdt").groupBy(
    window("time", "5 minutes", "1 minutes"), "key") \
    .agg(round(sum(values.quantity), 3).alias("sum_qty"), round(avg(values.price), 3).alias("avg_price"),
         round(max(values.price), 3).alias("max_price"), round(min(values.price), 3).alias("min_price"))

stream2_2 = stream2.select(stream2.window.start.cast(TimestampType()).alias("start_time"),
                           stream2.window.end.cast(TimestampType()).alias("end_time"), "key", "sum_qty", "avg_price",
                           col("max_price").cast("float"),
                           col("min_price").cast("float"))


# PostgreSQL setup
def batch_function(row, batch_id):
    # new_row = agg_row.select("start_time","end_time","key","sum-qty","avg-price","max-price","min-price")
    row.write.jdbc(url="jdbc:postgresql://127.0.0.1:5432/aggtrade", table="dotusdt_test", mode="append",
                   properties={"user": "postgres", "password": "123456"})


def batch_function_dot(row, batch_id):
    row.write.jdbc(url="jdbc:postgresql://127.0.0.1:5432/aggtrade", table="adausdt_test", mode="append",
                   properties={"user": "postgres", "password": "123456"})


url = "jdbc:postgresql://127.0.0.1:5432/aggtrade"
q1 = stream1_2.writeStream.foreachBatch(batch_function) \
    .outputMode("complete") \
    .option("checkpointLocation", "checkpoint/dbgrouping") \
    .start()

q2 = stream2_2.writeStream.foreachBatch(batch_function_dot) \
    .outputMode("complete") \
    .option("checkpointLocation", "checkpoint/dbgrouping2") \
    .start()

q1.awaitTermination()
q2.awaitTermination()
"""
For running SQL commands:
df.createOrReplaceTempView("deneme")
spark.sql("select * from deneme")
"""

# SPARK CONTEXT RDD
"""
sc = SparkContext(appName="deneme5").getOrCreate()
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)
kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "grup4", {"aggTrade": 3})

sqlContext = SQLContext(sc)

schema_kpq = StructType([
        StructField("key",StringType(),True),
        StructField("price",FloatType(),True),
        StructField("quantity",FloatType(),True)
    ])



#.option("checkpointLocation","checkpoint") \


parsed = kafkaStream.map(lambda v : (v[0],json.loads(v[1])))
#parsed.pprint()
# Hangi coinin windowlanan surecteki trade sayilarini sayiyor.
counts = parsed.map(lambda k : (k[0],1)).reduceByKeyAndWindow(lambda x,y : x+y, lambda x,y : x-y, 120 , 10)
#counts.pprint()

kpq = parsed.map(lambda pair : (pair[0],pair[1]['price'],pair[1]['quantity']))
sum_qty = kpq.map(lambda x : (x[0],x[2])).reduceByKeyAndWindow(lambda x,y : x+y,lambda x,y: x-y , 120,10)
avg_price = kpq.map(lambda x : (x[0],x[1])).reduceByKeyAndWindow(lambda x,y : (x+y)/2,120,10)
all = counts.union(sum_qty).union(avg_price)


ssc.checkpoint("checkpoint")
ssc.start()
ssc.awaitTermination()"""
