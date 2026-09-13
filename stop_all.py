# -*- coding: utf-8 -*-
"""智慧农业监测系统 —— 一键停止
用法: 双击运行, 或 py stop_all.py
停止顺序: Flask -> 生产者 -> Spark -> Kafka -> ZooKeeper -> HDFS

说明: 用户习惯直接关虚拟机, 那样 HDFS/Kafka 会残留脏状态。
      用本脚本停止可以干净关闭, 下次启动就不会遇到
      集群ID不一致 / 安全模式 / checkpoint 卡死等问题。
      (就算忘了用本脚本, start_all.py 里也有自动修复)
"""
import subprocess
import sys
import time

VM = "atguigu@192.168.10.100"


def run(cmd, cwd=None, timeout=90):
    try:
        r = subprocess.run(cmd, shell=True, cwd=cwd,
                           capture_output=True, text=True,
                           encoding="utf-8", errors="replace", timeout=timeout)
        return (r.stdout or "") + (r.stderr or "")
    except subprocess.TimeoutExpired:
        return "TIMEOUT"


def ssh(cmd, timeout=60):
    return run(f'ssh -o ConnectTimeout=8 {VM} "{cmd}"', timeout=timeout)


def kill_by_cmdline(keyword):
    ps = (f'Get-CimInstance Win32_Process | '
          f"Where-Object {{ $_.CommandLine -match '{keyword}' }} | "
          f'ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force '
          f'-ErrorAction SilentlyContinue }}')
    run(f'powershell -NoProfile -Command "{ps}"', timeout=30)


def main():
    print("=" * 46)
    print("  智慧农业监测系统 一键停止")
    print("=" * 46)

    print("\n[1/5] 停止 Flask ...")
    kill_by_cmdline(r"python.*app\.py|py.*app\.py")
    print("  [OK]")

    print("[2/5] 停止生产者 ...")
    kill_by_cmdline("kafka_producer_live")
    print("  [OK]")

    print("[3/5] 停止 Spark (虚拟机) ...")
    ssh("pkill -9 -f SparkSubmit 2>/dev/null; echo stopped", timeout=30)
    kill_by_cmdline("spark-submit")
    print("  [OK]")

    print("[4/5] 停止 Kafka 和 ZooKeeper ...")
    kill_by_cmdline("kafka.Kafka")
    time.sleep(3)
    kill_by_cmdline("QuorumPeerMain")
    time.sleep(2)
    # 兜底: 还有 java 进程占 9092/2181 就全杀
    out = run('netstat -ano | findstr "9092 2181" | findstr LISTENING', timeout=10)
    for line in out.splitlines():
        parts = line.split()
        if parts:
            run(f'taskkill /F /PID {parts[-1]}', timeout=10)
    print("  [OK]")

    print("[5/5] 停止 HDFS (虚拟机) ...")
    out = ssh("/opt/molude/hadoop-3.1.3/sbin/stop-dfs.sh", timeout=90)
    print("  [OK]" if "Stopping" in out or "TIMEOUT" not in out else "  [警告] 超时")

    print("\n" + "=" * 46)
    print("  全部停止! 现在可以安全关闭虚拟机了")
    print("=" * 46)
    try:
        input("\n按回车键关闭本窗口...")
    except EOFError:
        pass


if __name__ == "__main__":
    main()
