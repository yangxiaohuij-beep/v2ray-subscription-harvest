#!/bin/bash
# 每周全量重证启动器（供 Hermes cron 周日凌晨调用）
# 目的：全量要跑 20-60 分钟，同步执行有被 cron 超时打断的风险，
#       所以这里只负责 nohup 后台拉起，秒级返回，结果落盘供次日增量任务读取。
set -uo pipefail

RUN="${HARVEST_RUN_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
LOCK="$RUN/.lock"

if [ -e "$LOCK" ]; then
  PID=$(cat "$LOCK" 2>/dev/null)
  if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
    echo "全量采集已在运行（pid $PID），本次不重复启动。"
    exit 0
  fi
  rm -f "$LOCK"
fi

cd "$RUN" || { echo "无法进入 $RUN"; exit 1; }
mkdir -p logs

nohup env MODE=full ./run_daily.sh >"$RUN/logs/full-nohup-$(date +%Y%m%d).log" 2>&1 &
PID=$!
echo "=== 每周全量重证已启动 $(date '+%Y-%m-%d %H:%M') ==="
echo "pid $PID，后台运行，预计 20-60 分钟。"
echo "日志: $RUN/logs/full-$(date +%Y%m%d).log"
echo "结果会在 07:00 的增量任务里被读取并生成当日 v2rayN 版本。"
