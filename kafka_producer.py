
import json
import time
    
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")

with open("clean_data.json", "r", encoding="utf-8") as f:
    for i, line in enumerate(f):
        if i >= 10:
            break
        record = json.loads(line)
        # Kafka 消息值必须是 bytes，这里把 dict 手动转成 JSON bytes
        producer.send("sensor_data", value=json.dumps(record, ensure_ascii=False).encode("utf-8"))
        print("已发送:", record)
        time.sleep(0.01)

producer.flush()
producer.close()
print("发送完成")
