@echo off
chcp 65001 >nul
title SmartAgriSystem

echo ========================================
echo   Start Smart Agriculture System
echo ========================================
echo.

echo [1/7] Starting HDFS...
ssh atguigu@192.168.10.100 "/opt/molude/hadoop-3.1.3/sbin/start-dfs.sh"
timeout /t 5 /nobreak >nul

echo [2/7] Safe mode off + clean old data...
ssh atguigu@192.168.10.100 "hdfs dfsadmin -safemode leave; hadoop fs -rm -r -skipTrash /user/atguigu/sensor_out 2>/dev/null; hadoop fs -rm -r -skipTrash /user/atguigu/sensor_ckpt 2>/dev/null; echo Done"
timeout /t 3 /nobreak >nul

echo [3/7] Starting ZooKeeper...
cd /d C:\kafka
start "ZooKeeper" /MIN bin\windows\zookeeper-server-start.bat config\zookeeper.properties
timeout /t 8 /nobreak >nul

echo [4/7] Starting Kafka...
start "Kafka" /MIN bin\windows\kafka-server-start.bat config\server.properties
timeout /t 15 /nobreak >nul

echo [5/7] Starting Spark...
ssh atguigu@192.168.10.100 "pkill -9 -f SparkSubmit 2>/dev/null; sleep 2; cd /opt/molude/spark-3.0.0 && nohup ./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 /home/atguigu/kafka_stream.py > /tmp/spark-stream.log 2>&1 &"
timeout /t 10 /nobreak >nul

echo [6/7] Starting Producer...
cd /d "%~dp0"
start "Producer" /MIN C:\Windows\py.exe kafka_producer_live.py
timeout /t 3 /nobreak >nul

echo [7/7] Starting Flask...
start "Flask" /MIN C:\Windows\py.exe app.py
timeout /t 3 /nobreak >nul

echo.
echo ========================================
echo   All started!
echo   Visit: http://localhost:5000
echo   (wait ~60s for first data)
echo ========================================
echo.
pause
