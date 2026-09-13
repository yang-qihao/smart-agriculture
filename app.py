# -*- coding: utf-8 -*-
"""实时大屏实战应用 (:5000)
读取 Spark 写到 HDFS 的聚合结果文件, 提供 /api/latest 和 /api/health,
页面每 2 秒轮询刷新数值。"""
import json
import threading
import time
import requests
from flask import Flask, jsonify, render_template

app = Flask(__name__)
app.json.ensure_ascii = False

# Hadoop WebHDFS 接口 (虚拟机 NameNode 9870 端口)
WEBHDFS = "http://192.168.10.100:9870/webhdfs/v1"
OUT_DIR = "/user/atguigu/sensor_out"
USER = "user.name=atguigu"

latest = None   # 最近一次读到的聚合结果
_last_error = None  # 最近一次刷新错误


def hdfs_list_files(limit=20):
    """列出 HDFS 输出目录下的 part 文件, 只返回最新的 N 个"""
    r = requests.get(f"{WEBHDFS}{OUT_DIR}?op=LISTSTATUS&{USER}", timeout=10)
    r.raise_for_status()
    items = []
    for f in r.json()["FileStatuses"]["FileStatus"]:
        name = f["pathSuffix"]
        if name.startswith("part-"):
            items.append((f["modificationTime"], f"{OUT_DIR}/{name}"))
    items.sort()
    return items[-limit:]


def hdfs_read_text(path):
    """WebHDFS OPEN 读取整个文本文件"""
    r = requests.get(f"{WEBHDFS}{path}?op=OPEN&{USER}", timeout=5)
    r.raise_for_status()
    return r.content.decode("utf-8")


def parse_line(line):
    # Spark csv 输出: window_start,window_end,avg_temp,max_temp,min_temp
    p = line.strip().split(",")
    if len(p) < 5:
        return None
    try:
        return {"window_start": p[0], "window_end": p[1],
                "avg_temp": float(p[2]), "max_temp": float(p[3]),
                "min_temp": float(p[4])}
    except (ValueError, IndexError):
        return None


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/latest")
def api_latest():
    if latest is None:
        return jsonify({"error": "还没有读到数据", "status": "waiting", "last_error": _last_error}), 503
    return jsonify({"status": "ok", **latest})


@app.route("/api/health")
def api_health():
    try:
        files = hdfs_list_files()
        if not files:
            return jsonify({"status": "waiting", "msg": "HDFS 目录暂无输出文件, 等 Spark 出数"})
        newest = files[-1][1]
        text = hdfs_read_text(newest)
        rec = None
        for line in text.splitlines():
            rec = parse_line(line) or rec
        return jsonify({"status": "online", "file": newest.split("/")[-1],
                        "records": len(text.splitlines())})
    except requests.exceptions.RequestException as e:
        return jsonify({"status": "error", "msg": f"连接 HDFS 失败: {e}"}), 500


def _refresh_once():
    """读最新文件最后一行, 更新 latest"""
    global latest, _last_error
    try:
        files = hdfs_list_files(limit=20)
        if not files:
            _last_error = "HDFS 无文件"
            return
        # 从最新的文件开始往前找, 找到第一个有内容的
        for _, path in reversed(files):
            try:
                text = hdfs_read_text(path)
                for line in reversed(text.splitlines()):
                    rec = parse_line(line)
                    if rec:
                        latest = rec
                        _last_error = None
                        return
            except Exception:
                continue
        _last_error = "所有文件都是空的"
    except Exception as e:
        _last_error = str(e)


def _bg_refresh():
    """后台线程每 3 秒刷新一次"""
    while True:
        try:
            _refresh_once()
        except Exception as e:
            _last_error = f"后台刷新异常: {e}"
        time.sleep(3)


# 启动时先刷一次, 然后后台持续刷新
_refresh_once()
t = threading.Thread(target=_bg_refresh, daemon=True)
t.start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
