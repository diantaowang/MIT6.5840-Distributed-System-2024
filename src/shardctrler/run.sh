#!/bin/bash

# 配置参数
NUM_RUNS=2000         # 需要运行 40 次
MAX_PARALLEL=32     # 最多并行 32 个测试
LOG_DIR="test_logs" # 存放日志的目录
TIMEOUT="10m"       # 单个测试超时时间

# 创建日志目录
mkdir -p "$LOG_DIR"

echo "Running $NUM_RUNS Go tests with a max of $MAX_PARALLEL parallel jobs..."
echo "Logs will be saved in $LOG_DIR"

# 运行测试的后台任务数
running_jobs=0
    
# 运行 40 次测试
for i in $(seq 1 $NUM_RUNS); do
    LOG_FILE="$LOG_DIR/test_run_$i.log"
    echo "Starting test iteration $i (log: $LOG_FILE)..."

    # 启动一个测试并保存日志
    (go test -timeout=$TIMEOUT -count=1 -v ./... > "$LOG_FILE" 2>&1) &
    #(go test -timeout=$TIMEOUT -v -run TestBasic > "$LOG_FILE" 2>&1) &

    # 计数正在运行的测试
    ((running_jobs++))

    # 如果达到最大并行限制，等待至少一个任务完成
    if [[ $running_jobs -ge $MAX_PARALLEL ]]; then
        wait -n  # 等待任意一个任务结束
        ((running_jobs--))  # 释放一个线程名额
    fi
done

# 等待所有测试完成
wait

echo "All tests completed! Check logs in $LOG_DIR."
