# 变更记录

本文件记录项目所有值得写下来的变更。版本号从首次公开发布（2026-11-01）起算 v1.0，
此前的迭代按事实倒序记录为 v0.x。

## v1.0 — 2026-11-01

**公开发布**

- 新增全套文档：需求规格说明书、可行性分析、系统分析与架构设计、详细设计、运维与排障手册、数据质量与评测。
- 脚本可移植化：`run_daily.sh` / `run_full.sh` 不再写死绝对路径，
  改为 `RUN="${HARVEST_RUN_DIR:-脚本所在目录}"`，并支持 `HARVEST_PYTHON` / `HARVEST_GH` / `HARVEST_NO_PROXY`。
- 首次把当日产出（v2rayN 三件套 + Free V2rayU 包 + bundles 精选档）随仓库发布到 `releases/2026-11-01/`，
  并附数据快照 `data/sources-all.txt`（2804 个存活源）与 `data/repositories.txt`（808 个仓库）。
- 沙箱回归验证：编排顺序（harvest → rebuild → export → free）与失败隔离（采集失败不导出）两路径断言通过。

## v0.9 — 2026-10-31

**修复「当日包是昨日包」的结构性缺陷**

- 新增 `rebuild_bundles.py`：用 `subs/all.txt` 的存活源重新抓配置并重写 `subs/bundles/`。
- `run_daily.sh` 在导出前调用它；摘要新增 `bundles 重建 rc=` 字段。
- 保护策略：唯一端点 < `--min-total 500` 时拒绝写入（rc=3），保留旧包，不发空包。
- 触发原因：`daily --only-new` 只证到 2 个链接 → bundles 只有 70 条配置 → 导出的 best 包与前一日 md5 完全相同。
- 实测：2804 源 → 2798 有配置 → 76400 唯一端点 → 87s → 29 个文件。

## v0.8 — 2026-10-31

**修复 Free V2rayU 附加源的 SSL EOF**

- `fetch_freev2rayu.py` 增加代理探测：`api.github.com` 探不通就改用 `ProxyHandler({})` 直连；
  并给 `http_get()` 加 3 次重试（2s / 5s 退避）。
- 根因：macOS 系统代理开启且节点已死时，`urllib` 全部 SSL EOF，而 `curl` 正常。
- 同日站点恢复更新（文章 2026-10-30，4 个资源），补跑产出 749 条存活节点。

## v0.7 — 2026-10-30

**失败不再产出「伪当日包」**

- `run_daily.sh`：采集 `rc != 0` 时跳过 `make_v2rayn.py`（`VRC=99`），日志写 `⚠️ 采集失败 … 跳过 v2rayN 导出`。
- 触发原因：采集被外部 SIGTERM（rc=143）后脚本继续用旧 bundles 导出，产物齐全但全是旧数据，摘要却显示「候选 7392 / 存活 1665」。
- 同日补充：`harvest_lite.py` 的 `http_get()` 镜像优先回退（gh-proxy.com / ghproxy.net / ghfast.top / jsdelivr），
  解决 `raw.githubusercontent.com` 国内挂起（SYN_SENT）导致的「采集卡死」。

## v0.6 — 2026-10-28

- 新增附加源 `fetch_freev2rayu.py`：三步跟随（首页 → 文章 → 上传资源），base64 解码、协议白名单、内网/示例地址过滤、独立 TCP 探测。
- 附加源独立产出，不并入主 bundles，失败不影响主流程。

## v0.5 — 2026-10-27

- 新增 `make_v2rayn.py`：`bundles/` → v2rayN 三件套（best / mini / sources，均含 base64）。
- `build_mini()` 改为跨协议轮询 + 同主机最多 2 条，避免单一源/单一协议占满精选包。
- 增加占位/内网假节点过滤（127.x、10.x、example.com、全零 UUID 等）。

## v0.4 — 2026-10-27

- 新增 `run_daily.sh`（日跑编排：锁、token 注入、增量/全量、导出、附加源、摘要）与 `run_full.sh`（全量 nohup 后台启动器）。
- 定下「日常增量 + 每周全量」双轨策略，`--max-stale-days` 放宽到 14 天以适配增量模式。

## v0.3 — 2026-10-26

- `harvest_lite.py` 完成四阶段（search / collect / prove / write）与六项权重打分。
- 引入「摘要用解码后文本 sha256」以避免 base64 源被误判为每日更新。

## v0.1 — 2026-10-25

- 初版：复刻上游 `morpheusadam/v2ray-config` 的 `harvest.py` 订阅侧，纯标准库单文件实现。
