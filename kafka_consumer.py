import json

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "sensor_data",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",  # 只读取程序启动之后产生的新消息（earliest）
)

for i, msg in enumerate(consumer):
    record = json.loads(msg.value.decode("utf-8"))  # bytes -> dict
    print(f"收到第 {i + 1} 条:", record)
    if i >= 9:
        break

consumer.close()

