# 🌾 智慧农业管理系统 - 原理与运行教程

> 项目：模拟大棚传感器持续上报数据，经过 Kafka → Spark → HDFS → Flask 的完整大数据链路，最终在浏览器大屏实时展示温度变化。

---

## 一、系统原理

### 1. 整体架构（数据流向）

```
┌─────────────────────── Windows ───────────────────────┐
│                                                        │
│  kafka_producer_live.py                                │
│  (模拟传感器, 每2秒生成一条数据)                          │
│        │  温度/湿度/土壤湿度/光照强度 JSON                │
│        ▼                                               │
│  Kafka (localhost:9092, 依赖 ZooKeeper 2181)            │
│  topic: sensor_data                                    │
└────────┼───────────────────────────────────────────────┘
         │  通过局域网 192.168.10.1:9092 跨机器传输
┌────────▼──────────────── 虚拟机 hadoop100 ─────────────┐
│                                                        │
│  Spark Structured Streaming (kafka_stream.py)          │
│  · 从 Kafka 消费 sensor_data                            │
│  · 事件时间 + 1秒水位线                                   │
│  · 5秒滚动窗口聚合: avg/max/min 温度                      │
│  · checkpoint 存 HDFS /user/atguigu/sensor_ckpt         │
│        │  每批次结果写 CSV                                │
│        ▼                                               │
│  HDFS  /user/atguigu/sensor_out/part-*.csv             │
│  格式: window_start,window_end,avg_temp,max_temp,min_temp│
└────────┼───────────────────────────────────────────────┘
         │  WebHDFS REST 接口 (9870 端口)
┌────────▼─────────────────── Windows ───────────────────┐
│  Flask app.py (:5000)                                  │
│  · 后台线程每3秒读 HDFS 最新 CSV                          │
│  · /api/latest 返回最新聚合结果                           │
│  · / 渲染大屏页面, 前端每2秒轮询刷新                        │
└────────────────────────────────────────────────────────┘
```

### 2. 每个组件的作用

| 组件 | 位置 | 作用 |
|------|------|------|
| **生产者** kafka_producer_live.py | Windows | 模拟传感器，每 2 秒发一条 JSON（时间戳+温湿度+土壤湿度+光照）到 Kafka |
| **ZooKeeper** | Windows :2181 | Kafka 的"注册中心"，管理 broker 注册、topic 元数据、controller 选举 |
| **Kafka** | Windows :9092 | 消息队列，削峰缓冲。生产者只管发，Spark 按自己的节奏消费，两边速度解耦 |
| **Spark Streaming** kafka_stream.py | 虚拟机 | 流处理核心。5 秒窗口聚合温度（平均/最高/最低），结果写 HDFS |
| **HDFS** | 虚拟机 :8020/:9870 | 分布式存储，保存每批聚合结果 CSV。WebHDFS 提供 HTTP 读取接口 |
| **Flask** app.py | Windows :5000 | 大屏后端。轮询 HDFS 最新文件，通过 API 给前端页面 |

### 3. 关键概念解释

- **为什么用事件时间+水位线？** 生产者发的是"数据产生时刻"（time_stamp），Spark 按这个时间分窗口，而不是按"收到时刻"，网络延迟也不会算错窗口。水位线 1 秒 = 容忍 1 秒乱序。
- **为什么有 checkpoint？** Spark 记录"消费到 Kafka 哪个 offset、窗口状态是什么"，挂了能恢复。但**副作用**是：如果 Kafka 数据被清空重建，旧 checkpoint 对不上号，Spark 会卡死不出数——所以启动脚本每次会先删掉它（我们的数据是模拟的，删了无损）。
- **为什么 Kafka 要 advertised.listeners=192.168.10.1:9092？** 客户端先连 bootstrap 地址，Kafka 会把"真实可达地址"返回给客户端。虚拟机在另一台机器上，必须告诉它 Windows 的局域网 IP，否则虚拟机连不回 Kafka。

---

## 二、一键启动/停止（推荐）

脚本在 `C:\Users\Administrator\Desktop\智慧农业管理系统\`：

### 🚀 启动

1. 先在 VMware 里**开机虚拟机 hadoop100**，等它完全启动
2. **双击 `start_all.py`**（或命令行 `py start_all.py`）
3. 等它显示 `[OK] 实时数据正常!`，浏览器打开 **http://localhost:5000**

脚本自动完成（顺序很重要）：

```
[0/7] 检查虚拟机是否开机（ssh 22 端口）
[1/7] 启动 HDFS（start-dfs.sh）
[2/7] 退出 HDFS 安全模式 + 删除旧 checkpoint/旧输出（防 Spark 卡死）
[3/7] 启动 ZooKeeper（脏数据时自动清空重建）
[4/7] 启动 Kafka + 等 broker 真正就绪 + 建 topic sensor_data
[5/7] ssh 到虚拟机启动 Spark（自动杀旧进程）
[6/7] 启动生产者（先杀残留旧进程，并验证 Kafka 真的收到数据）
[7/7] 启动 Flask（端口被占先清）
最后   轮询 /api/latest，确认大屏有实时数据才算成功
```

> 💡 会弹出几个最小化的 CMD 窗口（ZooKeeper/Kafka/Spark/Producer/Flask），**别手动关它们**，关了服务就停了。
> 💡 首次启动到出数据约需 1~2 分钟（Spark 初始化 + 第一批 5 秒窗口 + 水位线延迟）。

### 🛑 停止

**双击 `stop_all.py`**，按依赖顺序干净关闭：

```
Flask → 生产者 → Spark(虚拟机) → Kafka → ZooKeeper → HDFS(虚拟机)
```

显示"全部停止"后即可安全关闭虚拟机。

> ⚠️ **强烈建议用 stop_all.py 再关虚拟机。** 直接强关虚拟机会让 HDFS/Kafka 残留脏状态，虽然 start_all.py 有自动修复，但干净关闭最省事。

---

## 三、手动启动（备用，理解每一步时用）

### 虚拟机侧（先做）

```bash
ssh atguigu@192.168.10.100   # 密码: 【你的密码】

start-dfs.sh                          # 启动 HDFS
jps                                   # 应看到 NameNode/DataNode/SecondaryNameNode
hdfs dfsadmin -safemode leave         # 退出安全模式（关键！否则 Spark 写不了）

cd /opt/molude/spark-3.0.0
./bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 \
  /home/atguigu/kafka_stream.py       # 启动 Spark（前台跑，Ctrl+C 停）

# 看日志: tail -f /tmp/spark-stream.log
# 验证:   jps 看到 SparkSubmit
```

### Windows 侧（后做）

```bat
:: 1. ZooKeeper（新开一个 cmd 窗口）
cd /d C:\kafka
bin\windows\zookeeper-server-start.bat config\zookeeper.properties

:: 2. Kafka（再开一个窗口）
cd /d C:\kafka
bin\windows\kafka-server-start.bat config\server.properties

:: 3. 建 topic（再开一个窗口，只需一次）
cd /d C:\kafka
bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 ^
  --create --if-not-exists --topic sensor_data --partitions 1 --replication-factor 1

:: 4. 生产者
cd /d C:\Users\Administrator\Desktop\智慧农业管理系统
py kafka_producer_live.py

:: 5. Flask 大屏（再开一个窗口）
cd /d C:\Users\Administrator\Desktop\智慧农业管理系统
py app.py
```

### 手动停止

```bash
# 虚拟机
jps | grep SparkSubmit   # 拿 PID
kill -9 <PID>
stop-dfs.sh
```
```bat
:: Windows: 直接关掉 Kafka/ZooKeeper/生产者/Flask 的 cmd 窗口即可
```

---

## 四、常见故障排查（全部实战踩过）

| # | 现象 | 根本原因 | 解决办法 |
|---|------|----------|----------|
| 1 | 大屏一直"等待数据"，API 返回 `window_end` 是很久以前的 | Spark 挂了 / 生产者挂了 / Kafka 没起，数据链路断了 | 按下面"逐段排查法"定位断点 |
| 2 | Spark 日志报 `SafeModeException: Cannot create directory ... Name node is in safe mode` | HDFS 重启后默认进安全模式，只读不可写 | `hdfs dfsadmin -safemode leave`，然后重启 Spark |
| 3 | Spark 进程活着、控制台窗口数据也在打印，但 HDFS 一直不写新文件 | 旧 checkpoint 记录了已不存在的 Kafka offset/目录状态，sink 提交卡死 | 删掉 `/user/atguigu/sensor_ckpt` 和 `/user/atguigu/sensor_out`，重启 Spark（start_all.py 已自动做） |
| 4 | Kafka 启动即崩，日志报 `InconsistentClusterIdException: Cluster ID doesn't match stored clusterId in meta.properties` | ZooKeeper 数据被清过，但 Kafka 磁盘上还有旧集群 ID | 删除 `C:\kafka\kafka-logs\meta.properties` 和 `C:\kafka\data\meta.properties`（或整个清空 kafka-logs + zk-data），重启 ZK 和 Kafka |
| 5 | Kafka 崩溃报 `NodeExistsException` | 上次 Kafka 没干净关闭，ZK 里的 broker 临时节点还没过期 | 等 20 秒重试，或清空 zk-data 重启 |
| 6 | 生产者显示"已推送"，但 Kafka 里没消息（consumer 收不到），Python 报 `KafkaTimeoutError` / 日志刷 `NotLeaderForPartitionError` | topic 的分区没有 leader（ZK/Kafka 数据不一致的后遗症） | 同 #4：清空 Kafka+ZK 数据重建 topic |
| 7 | Kafka 端口 9092 已监听，但发消息超时 | 端口通 ≠ broker 可用，日志段还在加载 | 多等 20~30 秒，用 `kafka-topics.bat --list` 能成功返回才算就绪 |
| 8 | 重启过 Kafka 后生产者"看起来在跑"但数据不进 Kafka | 旧生产者进程缓存了旧 broker 连接，一直在给死连接发消息 | 杀掉所有 python 生产者进程重启（start_all.py 已自动做） |
| 9 | Flask 报 `连接 HDFS 失败 ... 9870 拒绝连接` | 虚拟机没开机 / NameNode 没起 / WebHDFS 没开 | 开虚拟机 → `jps` 确认 NameNode → `start-dfs.sh` |
| 10 | 虚拟机 Spark 报 `Connection refused 192.168.10.1:9092` | Windows Kafka 没起，或防火墙拦了 | 启动 Kafka；检查 Windows 防火墙放行 9092 |
| 11 | `ModuleNotFoundError: No module named 'kafka'` | 缺依赖 | `pip install kafka-python flask requests` |
| 12 | 双击 .bat 报一堆 `'xxx' 不是内部或外部命令` | .bat 文件是 UTF-8 编码，cmd 按 GBK 解析中文全乱 | .bat 必须存成 **GBK/ANSI** 编码（本项目已改用 .py 脚本，无此问题） |
| 13 | 浏览器白屏 / 数据不动 | 前端缓存 | `Ctrl + F5` 强刷 |

### 逐段排查法（数据链路断在哪一段？）

按数据流方向逐段验证，哪段不通修哪段：

```bash
# ① 生产者在跑吗？
tasklist | findstr python
# 应该有跑 kafka_producer_live.py 的进程

# ② Kafka 收到消息了吗？
cd /d C:\kafka
bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 ^
  --topic sensor_data --max-messages 2 --timeout-ms 10000
# 能打印 JSON = ①② 正常

# ③ Spark 活着吗？在消费吗？
ssh atguigu@192.168.10.100 "jps | grep SparkSubmit"
ssh atguigu@192.168.10.100 "tail -30 /tmp/spark-stream.log"
# 日志里 Batch 数字在增长、表格有数据 = ③ 正常

# ④ HDFS 有新文件吗？
ssh atguigu@192.168.10.100 "hadoop fs -ls -t /user/atguigu/sensor_out/ | head -5"
# 最新文件时间是"刚刚" = ④ 正常

# ⑤ Flask API 返回新数据吗？
curl http://localhost:5000/api/latest
# window_end 距当前时间 2 分钟内 = 全链路正常
```

---

## 五、验证清单

启动完成后逐项检查：

- [ ] 虚拟机 `jps`：NameNode、DataNode、SecondaryNameNode、SparkSubmit 都在
- [ ] Windows 端口监听：2181（ZK）、9092（Kafka）、5000（Flask）
- [ ] Kafka 消费测试能打印传感器 JSON
- [ ] HDFS 有新生成的 CSV：`hadoop fs -ls /user/atguigu/sensor_out/`
- [ ] `curl http://localhost:5000/api/latest` 返回 `status: ok` 且 `window_end` 是最近 2 分钟内
- [ ] 浏览器 http://localhost:5000 大屏数字每几秒在跳

---

## 六、关键文件说明

| 文件 | 位置 | 作用 |
|------|------|------|
| `start_all.py` | Windows 项目目录 | ⭐ 一键启动（含全部自动修复） |
| `stop_all.py` | Windows 项目目录 | ⭐ 一键停止（按依赖顺序干净关闭） |
| `kafka_producer_live.py` | Windows 项目目录 | 模拟传感器，每 2 秒推一条 JSON 到 Kafka |
| `app.py` | Windows 项目目录 | Flask 大屏后端 (:5000)，轮询 WebHDFS 读最新结果 |
| `templates/dashboard.html` | Windows 项目目录 | 大屏前端页面，每 2 秒轮询 /api/latest |
| `kafka_stream.py` | 虚拟机 `/home/atguigu/` | Spark 流处理：Kafka→5秒窗口聚合→HDFS CSV |
| `server.properties` | `C:\kafka\config\` | Kafka 配置，关键项 `advertised.listeners=PLAINTEXT://192.168.10.1:9092` |
| `zookeeper.properties` | `C:\kafka\config\` | ZK 配置，数据目录 `c:/kafka/zk-data` |

### 重要路径备忘

- Kafka：`C:\kafka`（日志 `C:\kafka\logs\server.log`，数据 `C:\kafka\kafka-logs`）
- ZooKeeper 数据：`C:\kafka\zk-data`
- 虚拟机 Hadoop：`/opt/molude/hadoop-3.1.3`（注意目录名是 **molude** 不是 module）
- 虚拟机 Spark：`/opt/molude/spark-3.0.0`
- HDFS 输出：`/user/atguigu/sensor_out`
- Spark checkpoint：`/user/atguigu/sensor_ckpt`
- Spark 日志：虚拟机 `/tmp/spark-stream.log`
- Windows 主机局域网 IP（虚拟机 NAT 网关侧）：`192.168.10.1`
- 虚拟机 IP：`192.168.10.100`（用户 atguigu）

---

## 七、其他演示页面

| 地址 | 内容 |
|------|------|
| http://localhost:5000 | 📈 实时监控大屏（本项目主页面） |
| http://localhost:5001 | ⚡ Socket.IO 推送演示（socketio_demo.py） |
| http://localhost:5002 | 📖 Flask 基础教学（demo_flask_basics.py） |
