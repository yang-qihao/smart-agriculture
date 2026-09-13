# -*- coding: utf-8 -*-
"""Socket.IO 推送演示 (:5001)
后台线程每 2 秒模拟一条传感器数据, 通过 Socket.IO 主动推给浏览器,
用来体会「拉(REST轮询)」和「推(WebSocket)」的区别。"""
import random
import threading
import time
from datetime import datetime

from flask import Flask, jsonify, render_template
from flask_socketio import SocketIO

app = Flask(__name__)
app.config["SECRET_KEY"] = "socketio-demo"
app.json.ensure_ascii = False
sio = SocketIO(app, cors_allowed="*")

latest = None   # 最近一条数据
history = []    # 最近 60 条, 给折线图用


def gen():
    return {
        "time_stamp": datetime.now().strftime("%Y%m%d-%H%M%S"),
        "tem": round(random.uniform(15, 35), 2),
        "hum": round(random.uniform(40, 90), 2),
        "soil_hum": round(random.uniform(20, 80), 2),
        "light_inten": round(random.uniform(0, 10000), 2),
    }


def push_loop():
    global latest
    while True:
        latest = gen()
        history.append(latest)
        if len(history) > 60:
            history.pop(0)
        sio.emit("sensor_update", latest)   # 服务器主动推
        time.sleep(2)


@app.route("/")
def index():
    return render_template("demo_socketio.html")


@app.route("/api/latest")
def api_latest():
    if latest is None:
        return jsonify({"error": "后台线程还没采到数据"}), 503
    return jsonify(latest)


@app.route("/api/health")
def api_health():
    return jsonify({"has_data": latest is not None, "count": len(history)})


threading.Thread(target=push_loop, daemon=True).start()

if __name__ == "__main__":
    sio.run(app, host="0.0.0.0", port=5001, debug=True, use_reloader=False)
