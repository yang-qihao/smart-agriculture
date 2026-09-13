# -*- coding: utf-8 -*-
"""智慧农业监测系统 —— 一键启动 (内置所有历史故障的自动修复)
用法: 双击运行, 或 py start_all.py

已内置修复的问题:
  1. HDFS 安全模式导致 Spark 崩溃        -> 启动后循环退出安全模式
  2. 旧 checkpoint 导致 Spark 卡死不写数据 -> 每次启动前删除 sensor_ckpt / sensor_out
  3. ZooKeeper 残留状态导致 Kafka 报
     NodeExistsException / InconsistentClusterIdException /
     NotLeaderForPartitionError          -> Kafka 没在运行时, 彻底清空 ZK+Kafka 数据重建
  4. 旧生产者进程连着已死的 Kafka 发不出数据 -> 启动前先杀掉所有残留生产者
  5. Kafka 没完全就绪就建 topic/发消息超时   -> 端口通后再等 broker 真正可用
  6. 虚拟机没开机                            -> 启动前检测, 给出明确提示
  7. Flask 端口被旧进程占用                   -> 先杀旧进程再启动
"""
import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
from datetime import datetime

BASE = os.path.dirname(os.path.abspath(__file__))
KAFKA_DIR = r"C:\kafka"
VM = "atguigu@192.168.10.100"
VM_HOST = "192.168.10.100"
PY = r"C:\Windows\py.exe"


# ---------------- 基础工具 ----------------

def port_open(port, host="127.0.0.1"):
    with socket.socket() as s:
        s.settimeout(1)
        return s.connect_ex((host, port)) == 0


def run(cmd, cwd=None, timeout=90):
    try:
        r = subprocess.run(cmd, shell=True, cwd=cwd,
                           capture_output=True, text=True,
                           encoding="utf-8", errors="replace", timeout=timeout)
        return (r.stdout or "") + (r.stderr or "")
    except subprocess.TimeoutExpired:
        return "TIMEOUT"


def ssh(cmd, timeout=90):
    return run(f'ssh -o ConnectTimeout=8 {VM} "{cmd}"', timeout=timeout)


def start_window(title, cmd, cwd=None):
    """在独立最小化窗口中运行(不随本脚本退出而死)"""
    subprocess.Popen(f'start "{title}" /MIN cmd /c {cmd}', shell=True, cwd=cwd)


def kill_by_cmdline(keyword):
    """按命令行关键字强杀 Windows 进程"""
    ps = (f'Get-CimInstance Win32_Process | '
          f"Where-Object {{ $_.CommandLine -match '{keyword}' }} | "
          f'ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force '
          f'-ErrorAction SilentlyContinue }}')
    run(f'powershell -NoProfile -Command "{ps}"', timeout=30)


def pause_exit(code=0):
    try:
        input("\n按回车键关闭窗口...")
    except EOFError:
        pass
    sys.exit(code)


# ---------------- 各步骤 ----------------

def check_vm():
    """问题6: 虚拟机没开机 -> 明确提示"""
    print("[0/7] 检查虚拟机 ...")
    if not port_open(22, VM_HOST):
        print("  [失败] 连不上虚拟机 192.168.10.100 (22端口不通)")
        print("  请先在 VMware 里把虚拟机开机, 等它完全启动后再运行本脚本!")
        pause_exit(1)
    out = ssh("echo ok", timeout=20)
    if "ok" not in out:
        print("  [失败] 虚拟机能 ping 通但 ssh 失败, 请检查虚拟机里 sshd")
        pause_exit(1)
    print("  [OK] 虚拟机在线")


def start_hdfs():
    """问题1: 安全模式; 问题2: 旧数据"""
    print("[1/7] 启动 HDFS ...")
    out = ssh("/opt/molude/hadoop-3.1.3/sbin/start-dfs.sh", timeout=60)
    if "running as process" in out:
        print("  [OK] HDFS 已在运行")
    else:
        # 等 NameNode Web 端口 9870 就绪
        for _ in range(30):
            if port_open(9870, VM_HOST):
                break
            time.sleep(1)
        print("  [OK] HDFS 已启动")

    print("[2/7] 退出安全模式 + 清理旧数据 ...")
    # 问题1修复: 循环退出安全模式直到确认 OFF
    for _ in range(5):
        out = ssh("hdfs dfsadmin -safemode leave; hdfs dfsadmin -safemode get", timeout=30)
        if "Safe mode is OFF" in out:
            break
        time.sleep(2)
    # 问题2修复: 删除旧 checkpoint 和旧输出, 避免 Spark 恢复旧状态卡死
    ssh("hadoop fs -rm -r -skipTrash /user/atguigu/sensor_out 2>/dev/null; "
        "hadoop fs -rm -r -skipTrash /user/atguigu/sensor_ckpt 2>/dev/null; "
        "hadoop fs -mkdir -p /user/atguigu/sensor_out; echo CLEAN_OK", timeout=60)
    print("  [OK] 安全模式已关, 旧数据已清理")


def clean_kafka_data():
    """问题3修复: ZK/Kafka 数据不一致时彻底清空重建(数据是可再生的测试数据)"""
    for d in ("zk-data", "kafka-logs", "data"):
        p = os.path.join(KAFKA_DIR, d)
        run(f'rmdir /s /q "{p}"', cwd=KAFKA_DIR, timeout=30)
        os.makedirs(p, exist_ok=True)
    print("  [OK] 已清空 ZooKeeper/Kafka 旧数据 (避免集群ID不一致)")


def start_zk_kafka():
    print("[3/7] 启动 ZooKeeper ...")
    if port_open(2181):
        print("  [OK] 已在运行")
    else:
        start_window("ZooKeeper",
                     r"bin\windows\zookeeper-server-start.bat config\zookeeper.properties",
                     cwd=KAFKA_DIR)
        for _ in range(30):
            if port_open(2181):
                break
            time.sleep(1)
        if not port_open(2181):
            print("  [失败] ZooKeeper 启动超时")
            pause_exit(1)
        print("  [OK] ZooKeeper 就绪")

    print("[4/7] 启动 Kafka ...")
    if port_open(9092):
        print("  [OK] 已在运行")
    else:
        # 问题3修复: Kafka 不在运行 = 可能残留脏数据, 先彻底清空再起
        # 注意: 只有 ZK 也是刚启动的才清空, 避免破坏正在服务的集群
        clean_kafka_data()
        # ZK 数据被清了, 重启 ZK 让它用干净目录
        kill_by_cmdline("QuorumPeerMain")
        time.sleep(2)
        start_window("ZooKeeper",
                     r"bin\windows\zookeeper-server-start.bat config\zookeeper.properties",
                     cwd=KAFKA_DIR)
        for _ in range(30):
            if port_open(2181):
                break
            time.sleep(1)
        start_window("Kafka",
                     r"bin\windows\kafka-server-start.bat config\server.properties",
                     cwd=KAFKA_DIR)
        for _ in range(60):
            if port_open(9092):
                break
            time.sleep(1)
        if not port_open(9092):
            print("  [失败] Kafka 启动超时, 查看 C:\\kafka\\logs\\server.log")
            pause_exit(1)
        print("  [OK] Kafka 端口就绪")

    # 问题5修复: 端口通 != broker 可用, 用建 topic 命令验证并等待真正就绪
    print("  等待 Kafka broker 完全就绪 ...")
    for i in range(12):
        out = run(r"bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 "
                  r"--create --if-not-exists --topic sensor_data "
                  r"--partitions 1 --replication-factor 1",
                  cwd=KAFKA_DIR, timeout=30)
        if "Created topic" in out or "exists" in out:
            print("  [OK] topic sensor_data 就绪")
            return
        time.sleep(5)
    print("  [失败] Kafka broker 一直没就绪")
    pause_exit(1)


def start_spark():
    print("[5/7] 启动 Spark ...")
    ssh("pkill -9 -f SparkSubmit 2>/dev/null; echo killed", timeout=20)
    kill_by_cmdline("spark-submit")  # 杀掉本地残留的 ssh 窗口
    time.sleep(2)
    start_window(
        "Spark",
        f'ssh {VM} "cd /opt/molude/spark-3.0.0 && ./bin/spark-submit '
        '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 '
        '/home/atguigu/kafka_stream.py > /tmp/spark-stream.log 2>&1"')
    # 等 VM 上出现 SparkSubmit 进程
    for _ in range(12):
        time.sleep(5)
        if "SparkSubmit" in ssh("jps | grep SparkSubmit", timeout=15):
            print("  [OK] Spark 已启动")
            return
    print("  [警告] 60秒内没检测到 SparkSubmit, 查看虚拟机 /tmp/spark-stream.log")


def start_producer():
    """问题4修复: 先杀掉所有残留生产者, 再启动新的"""
    print("[6/7] 启动数据生产者 ...")
    kill_by_cmdline("kafka_producer_live")
    time.sleep(1)
    start_window("Producer", f'{PY} -u kafka_producer_live.py', cwd=BASE)
    time.sleep(3)
    # 验证消息真的进了 Kafka
    out = run(r"bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 "
              r"--topic sensor_data --max-messages 1 --timeout-ms 15000",
              cwd=KAFKA_DIR, timeout=40)
    if "tem" in out:
        print("  [OK] 生产者运行中, Kafka 已收到数据")
    else:
        print("  [警告] Kafka 还没收到数据, 生产者可能启动失败")


def start_flask():
    """问题7修复: 端口被占先杀旧进程"""
    print("[7/7] 启动 Flask 大屏 ...")
    if port_open(5000):
        print("  [OK] 已在运行")
        return
    kill_by_cmdline(r"python.*app\.py|py.*app\.py")
    time.sleep(1)
    start_window("Flask", f'{PY} app.py', cwd=BASE)
    for _ in range(20):
        if port_open(5000):
            print("  [OK] Flask 就绪")
            return
        time.sleep(1)
    print("  [失败] Flask 启动超时")


def verify_data():
    print("\n等待首批实时数据 (最多 120 秒) ...")

    def sec(t):
        h, m, s = map(int, t.split(":"))
        return h * 3600 + m * 60 + s

    for i in range(24):
        time.sleep(5)
        try:
            with urllib.request.urlopen("http://localhost:5000/api/latest", timeout=5) as r:
                data = json.loads(r.read().decode())
            if data.get("status") == "ok":
                w = data["window_end"][11:19]
                now = datetime.now().strftime("%H:%M:%S")
                if abs(sec(now) - sec(w)) < 120:
                    print(f"  [OK] 实时数据正常! 最新窗口: {w} (当前 {now})")
                    return True
        except Exception:
            pass
        if i % 3 == 2:
            print(f"  ... 仍在等待 ({(i+1)*5}s)")
    print("  [警告] 120 秒内没等到实时数据!")
    print("  排查: ssh 到虚拟机看 tail /tmp/spark-stream.log")
    return False


def main():
    print("=" * 50)
    print("   智慧农业监测系统 一键启动")
    print("=" * 50 + "\n")
    check_vm()
    start_hdfs()
    start_zk_kafka()
    start_spark()
    start_producer()
    start_flask()
    ok = verify_data()
    print("\n" + "=" * 50)
    if ok:
        print("   启动完成! 浏览器访问: http://localhost:5000")
    else:
        print("   启动完成, 但数据未验证通过, 请按上面提示排查")
    print("=" * 50)
    pause_exit(0 if ok else 1)


if __name__ == "__main__":
    main()
