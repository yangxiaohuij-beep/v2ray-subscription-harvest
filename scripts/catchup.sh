#!/bin/bash
# 当日补跑：当 07:00 的日跑被调度器超时杀掉、但 search/collect 已完成时使用。
#
# 跳过 search/collect（候选池已生成），只做：prove → write → 重建 bundles → 导出 → 附加源。
# 用法：  ./catchup.sh [YYYYMMDD]        （省略日期则用今天）
#
# 与 run_daily.sh 的区别：不重新收集候选，通常 25~35 分钟跑完；建议放在 nohup/后台里跑，
# 避免被调用方的进程组超时一并杀掉。
set -uo pipefail

RUN="${HARVEST_RUN_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
PY="${HARVEST_PYTHON:-/usr/bin/python3}"
STAMP="${1:-$(date +%Y%m%d)}"
LOG="$RUN/logs/catchup-$STAMP.log"

cd "$RUN" || { echo "无法进入 $RUN"; exit 1; }
mkdir -p logs out
start=$(date +%s)
echo "=== 补跑开始 $(date '+%H:%M:%S')（$STAMP）===" >>"$LOG"

"$PY" -u harvest_lite.py subs prove --workers 40 --timeout 8 --tcp-timeout 2 \
  --max-stale-days 14 --only-new --no-recurse --samples 12 >>"$LOG" 2>&1
rc_prove=$?
echo "--- prove rc=$rc_prove $(date '+%H:%M:%S') ---" >>"$LOG"

if [ "$rc_prove" -eq 0 ]; then
  "$PY" -u harvest_lite.py subs write --max-stale-days 14 >>"$LOG" 2>&1
  echo "--- write rc=$? $(date '+%H:%M:%S') ---" >>"$LOG"

  "$PY" -u rebuild_bundles.py --workers 40 --timeout 8 --min-total 500 >>"$LOG" 2>&1
  echo "--- rebuild rc=$? $(date '+%H:%M:%S') ---" >>"$LOG"

  "$PY" -u make_v2rayn.py subs/bundles out >>"$LOG" 2>&1
  echo "--- export rc=$? $(date '+%H:%M:%S') ---" >>"$LOG"
else
  echo "⚠️ prove 失败，后续步骤跳过" >>"$LOG"
fi

"$PY" -u fetch_freev2rayu.py --stamp "$STAMP" >>"$LOG" 2>&1
echo "--- freev2rayu rc=$? $(date '+%H:%M:%S') ---" >>"$LOG"

echo "=== 补跑结束 $(date '+%H:%M:%S')，总耗时 $(( $(date +%s) - start ))s ===" >>"$LOG"
tail -20 "$LOG"
