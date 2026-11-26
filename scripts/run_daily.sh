#!/bin/bash
# 每日订阅节点收割（Hermes cron 调用）——skill: github-repo-subscription-harvest
#
# MODE=daily（默认）  增量：--only-new，只证明新出现的链接，几分钟走完
# MODE=full           全量：重证所有候选，约 20-60 分钟，供周日后台跑
#
# 为什么日常不用全量：6102 个候选里绝大多数是死链，每个都要等满超时，
# 全量在 16 并发 25s 超时下实测要 9 小时。日常增量 + 每周全量兜底是正确取舍。
# 为什么 --max-stale-days 放宽到 14：--only-new 不刷新老链接的 lastAlive，
# 保持 12 天会让它们在第 13 天被判 "no configs" 退役；放宽到 14 天并配周日全量即可安全。
set -uo pipefail

RUN="${HARVEST_RUN_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
PY="${HARVEST_PYTHON:-/usr/bin/python3}"
GH="${HARVEST_GH:-/opt/homebrew/bin/gh}"
MODE="${MODE:-daily}"
cd "$RUN" || { echo "无法进入 $RUN"; exit 1; }

mkdir -p "$RUN/logs" "$RUN/out"
STAMP="$(date +%Y%m%d)"
LOCK="$RUN/.lock"

# --- 防重入：04:00 的全量还没跑完时，07:00 的增量不要再插一脚 ---
if [ -e "$LOCK" ]; then
  PID=$(cat "$LOCK" 2>/dev/null)
  if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
    echo "=== 订阅节点每日收割 $STAMP ==="
    echo "跳过：另一个采集进程（pid ${PID}）仍在运行"
    exit 0
  fi
  rm -f "$LOCK"
fi

if [ "$MODE" = "full" ]; then
  LOG="$RUN/logs/full-$STAMP.log"
  EXTRA="--retired-days 30"
  LABEL="全量重证"
else
  LOG="$RUN/logs/daily-$STAMP.log"
  # --max-new 600：每天最多证 600 条新链接，候选暴涨时不会把调度器 3600s 硬超时顶穿
  # --retired-days 30：近期退役的源不再被 collect 收回来重证（2026-11-26 事故后的护栏）
  EXTRA="--only-new --max-new 600 --retired-days 30"
  LABEL="增量"
fi
echo $$ > "$LOCK"
trap 'rm -f "$LOCK"' EXIT
START=$(date +%s)

# --- GitHub token：只注入环境变量，不落盘 ---
AUTH="未认证(慢)"
if [ -x "$GH" ]; then
  TOK="$("$GH" auth token 2>/dev/null || true)"
  if [ -n "$TOK" ]; then
    export GITHUB_TOKEN="$TOK"
    AUTH="已认证"
  fi
fi

# --- 主流程 ---
# shellcheck disable=SC2086
"$PY" -u harvest_lite.py daily \
  --workers 40 --timeout 8 --tcp-timeout 2 \
  --max-stale-days 14 --no-guess --no-recurse $EXTRA >>"$LOG" 2>&1
RC=$?

# --- 导出 v2rayN 版（采集失败则拒绝导出，避免把旧 bundles 重导成「今日包」）---
# 增量模式（--only-new）只证新链接，bundles 会塌成几十条，而 make_v2rayn 读的是
# bundles/best.txt（本轮未重写）→ 当日包与昨日 md5 相同。所以导出前先用 all.txt
# 的存活源重建一次 bundles（2026-10-31 起）。
if [ "$RC" -ne 0 ]; then
  echo "⚠️ 采集失败 rc=$RC，跳过 v2rayN 导出（out/ 保持上次产物，不生成伪当日包）" >>"$LOG"
  VRC=99
  RRC=99
else
  "$PY" -u rebuild_bundles.py --workers 40 --timeout 8 --min-total 500 >>"$LOG" 2>&1
  RRC=$?
  if [ "$RRC" -ne 0 ]; then
    echo "⚠️ bundles 重建 rc=$RRC（3=唯一配置太少，已保护性保留旧 bundles），继续用旧 bundles 导出" >>"$LOG"
  fi
  "$PY" -u make_v2rayn.py "$RUN/subs/bundles" "$RUN/out" >>"$LOG" 2>&1
  VRC=$?
fi

# --- 附加源：Free V2rayU 站点（独立产出，不并入主 bundles；失败不影响主流程）---
"$PY" -u fetch_freev2rayu.py --stamp "$STAMP" >>"$LOG" 2>&1
FRC=$?

END=$(date +%s)
MINS=$(( (END - START) / 60 + 1 ))

# --- 精简摘要（no_agent 模式下这段 stdout 会被逐字投递成汇报）---
echo "=== 订阅节点每日收割 ${STAMP}（${LABEL}）==="
if [ "$RC" -ne 0 ]; then
  echo "⚠️ 采集失败 daily rc=$RC —— 详见 logs/$(basename "$LOG") 最后几行"
fi
echo "状态: daily rc=$RC, bundles 重建 rc=${RRC:-0}, 导出 rc=$VRC, 耗时约 ${MINS} 分钟, GitHub $AUTH"
FV_N=$(wc -l < "$RUN/out/freev2rayu-$STAMP.txt" 2>/dev/null | tr -d ' ')
echo "附加源 FreeV2rayU: rc=$FRC, ${FV_N:-0} 条 TCP 存活"
echo "仓库: $(wc -l < "$RUN/rapo.txt" 2>/dev/null | tr -d ' ') 个"
echo "候选: $(wc -l < "$RUN/subs/candidates.txt" 2>/dev/null | tr -d ' ') 个链接"
echo "订阅源存活: $(grep -vc '^#' "$RUN/subs/all.txt" 2>/dev/null) 个"
echo
if [ -d "$RUN/out" ]; then
  echo "--- 今日产出 ---"
  ls -1 "$RUN/out" | grep "$STAMP" | sed 's/^/  /'
  echo "路径: $RUN/out/"
  MINI="$RUN/out/v2rayN-$STAMP-mini-base64.txt"
  if [ -f "$MINI" ]; then
    echo
    echo "MEDIA:$MINI"
  fi
fi
echo
BUNDLE_ALL="$RUN/subs/bundles/all.txt"
if [ -f "$BUNDLE_ALL" ]; then
  echo "配置池: $(grep -vc '^#' "$BUNDLE_ALL" 2>/dev/null) 条（bundles 生成于 $(date -r "$BUNDLE_ALL" '+%m-%d %H:%M' 2>/dev/null)）"
fi
echo
echo "--- 日志尾部 ---"
tail -5 "$LOG"
exit $RC
