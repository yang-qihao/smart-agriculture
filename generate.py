import json
import random
from datetime import datetime


def generate_data():
    #百分之10的概率去生成一个空数据
    if random.random() < 0.1:
        return    {
        "time_stamp":datetime.now().strftime("%Y%m%d-%H%M%S")
    }

    data = {

        "time_stamp":datetime.now().strftime("%Y%m%d-%H%M%S"),
        "tem": round(random.uniform(15,35),2),
        "hum":round(random.uniform(40,90),2),
        "soil_hum":round(random.uniform(20,80),2),
        "light_inten":round(random.uniform(0,10000),2)
    }

    if random.random() < 0.05:
        data['tem'] = random.choice([-99,200])

    return data

if __name__ == "__main__":
    with open("dirty_data.json","w",encoding="utf-8") as f:
        for i  in range(100):
            record = generate_data()
            f.write(json.dumps(record,ensure_ascii=False) + "\n")

    print("原始数据已生成")