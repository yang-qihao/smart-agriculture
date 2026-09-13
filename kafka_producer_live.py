# -*- coding: utf-8 -*-
"""实时版生产者: 每 2 秒生成一条"当前时间"的传感器数据推送到 Kafka
(模拟真实大棚传感器持续上报, 让 Spark 的事件时间水位线能推进)"""
import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")


def gen():
    return {
        "time_stamp": datetime.now().strftime("%Y%m%d-%H%M%S"),
        "tem": round(random.uniform(15, 35), 2),
        "hum": round(random.uniform(40, 90), 2),
        "soil_hum": round(random.uniform(20, 80), 2),
        "light_inten": round(random.uniform(0, 10000), 2),
    }


if __name__ == "__main__":
    print("实时推送中 (Ctrl+C 停止)...")
    while True:
        record = gen()
        producer.send("sensor_data", value=json.dumps(record, ensure_ascii=False).encode("utf-8"))
        print("已推送:", record)
        time.sleep(2)
