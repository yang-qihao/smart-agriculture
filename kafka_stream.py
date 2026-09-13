# -*- coding: utf-8 -*-
"""Kafka -> Spark Structured Streaming -> HDFS
从 Windows Kafka 消费传感器数据, 5 秒窗口聚合温度,
结果以 csv 追加写入 HDFS /user/atguigu/sensor_out (供 Flask 大屏读取)"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, max, min, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("KafkaSensorStreamToHDFS").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. 从 Kafka 读流 (Windows 真实 NAT 地址: 192.168.10.1)
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.10.1:9092")
    .option("subscribe", "sensor_data")
    .option("startingOffsets", "latest")
    .load()
)

# 2. 定义 Schema
schema = StructType([
    StructField("time_stamp", StringType(), True),
    StructField("tem", DoubleType(), True),
    StructField("hum", DoubleType(), True),
    StructField("soil_hum", DoubleType(), True),
    StructField("light_inten", DoubleType(), True)
])

# 3. 解析 JSON
parsed_df = (
    kafka_df.select(from_json(col("value").cast("STRING"), schema).alias("data"))
    .select("data.*")
)

# 4. 字符串时间 -> 时间戳, 水位线
parsed_df = parsed_df.withColumn(
    "event_time", to_timestamp(col("time_stamp"), "yyyyMMdd-HHmmss")
).withWatermark("event_time", "1 seconds")

# 5. 5 秒窗口聚合
aggregated_df = (
    parsed_df
    .groupBy(window(col("event_time"), "5 seconds"))
    .agg(
        avg("tem").alias("avg_temp"),
        max("tem").alias("max_temp"),
        min("tem").alias("min_temp")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "avg_temp", "max_temp", "min_temp"
    )
)

# 6. 双输出: 控制台打印 + 追加写 HDFS (csv 无 schema 文件, Flask 好读)
query = (
    aggregated_df.writeStream
    .outputMode("append")   # csv 落盘只支持 append
    .format("csv")
    .option("path", "hdfs://192.168.10.100:8020/user/atguigu/sensor_out")
    .option("checkpointLocation", "hdfs://192.168.10.100:8020/user/atguigu/sensor_ckpt")
    .trigger(processingTime="5 seconds")
    .start()
)

# 同时在控制台保留打印(方便演示截图)
print_query = (
    aggregated_df.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()
