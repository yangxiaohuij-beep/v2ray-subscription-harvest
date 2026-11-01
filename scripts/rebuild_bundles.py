#!/usr/bin/env python3
"""用 subs/all.txt 里的全部存活源重建 subs/bundles/（让每日主包始终来自当天存活源）。

背景
----
`harvest_lite.py daily` 用 `--only-new` 只重证**新**链接，写 bundles 时手里只有本轮
证到的配置（2026-10-31 实测：50 个链接 → 1 个带配置 → 70 条配置，而上一次全量是
64260 条）。而 `make_v2rayn.py` 读的是 `subs/bundles/best.txt`，该文件本轮未被重写，
于是「当日 v2rayN 包」与上一日 md5 完全相同——文件名是新的，内容是旧的。

本脚本按 `subs/all.txt` 的顺序（分数降序）把每个存活源重新抓一遍，取其中的配置 URI，
端点去重后调用 `harvest_lite.write_bundles()` 重写整个 bundles/ 目录。

用法
----
    python3 rebuild_bundles.py [--workers 40] [--timeout 8] [--per-source 400]
                              [--max-bytes 524288] [--min-total 500]
                              [--limit 0] [--dry-run]

退出码
------
    0  重建成功
    3  抓到的唯一配置数低于 --min-total（保护：不覆盖旧 bundles，宁可留旧也不发空包）
    1  参数/IO 错误
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import harvest_lite as H  # noqa: E402  （复用同一套 classify/parse_endpoint/write_bundles）


def read_sources(path: Path) -> list[str]:
    """all.txt 一行一个 URL；# 开头是注释。保持原顺序（分数降序）。"""
    urls: list[str] = []
    seen: set[str] = set()
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line in seen:
            continue
        seen.add(line)
        urls.append(line)
    return urls


def check(url: str, *, timeout: int, max_bytes: int, per_source: int):
    response = H.http_get(url, max_bytes=max_bytes, timeout=timeout)
    verdict = H.classify(response)
    if verdict.kind != "configs":
        return url, verdict
    verdict.uris = H.CONFIG_URI_RE.findall(verdict.body)[:per_source]
    verdict.body = ""
    return url, verdict


def main() -> int:
    parser = argparse.ArgumentParser(description="从 subs/all.txt 重建 subs/bundles/")
    parser.add_argument("--workers", type=int, default=40, help="并发抓取数（默认 40）")
    parser.add_argument("--timeout", type=int, default=8, help="单源超时秒（默认 8）")
    parser.add_argument("--per-source", type=int, default=400, help="单源最多取多少条配置")
    parser.add_argument("--max-bytes", type=int, default=512 * 1024, help="单源最多读多少字节")
    parser.add_argument("--min-total", type=int, default=500,
                        help="唯一配置少于这个数就放弃写入（默认 500）")
    parser.add_argument("--limit", type=int, default=0, help="只抓前 N 个源（调试用）")
    parser.add_argument("--dry-run", action="store_true", help="只统计不写文件")
    args = parser.parse_args()

    if not H.SUBS_FILE.exists():
        H.log(f"找不到 {H.SUBS_FILE}，先跑 harvest collect/prove")
        return 1
    sources = read_sources(H.SUBS_FILE)
    if args.limit:
        sources = sources[:args.limit]
    if not sources:
        H.log("subs/all.txt 里没有源，放弃")
        return 1

    H.log(f"=== 从 subs/all.txt 重建 bundles（{len(sources)} 个存活源）===")
    started = time.time()
    results: list[tuple[str, object]] = []
    alive = 0
    with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(check, url, timeout=args.timeout,
                               max_bytes=args.max_bytes, per_source=args.per_source)
                   for url in sources]
        for index, future in enumerate(cf.as_completed(futures), 1):
            try:
                url, verdict = future.result()
            except Exception:
                continue
            results.append((url, verdict))
            if verdict.kind == "configs":
                alive += 1
            if index % 200 == 0 or index == len(futures):
                H.log(f"  [{index}/{len(futures)}] {alive} 个源携带配置")

    endpoints: set[tuple[str, int]] = set()
    for _, verdict in results:
        for uri in verdict.uris:
            endpoint = H.parse_endpoint(uri)
            if endpoint is not None:
                endpoints.add(endpoint)

    elapsed = time.time() - started
    H.log(f"抓完：{alive}/{len(sources)} 个源有配置，唯一端点 {len(endpoints)} 个，耗时 {elapsed:.0f}s")

    if len(endpoints) < args.min_total:
        H.log(f"唯一端点 {len(endpoints)} < 阈值 {args.min_total}，保留旧 bundles（不写入）")
        return 3
    if args.dry_run:
        H.log("dry-run：不写文件")
        return 0

    status = H.load_json(H.SUBS_STATUS, {"generatedAt": "", "links": {}})
    # write_bundles 用 results 的顺序 + status 里的 score 排序，两者口径一致
    ordered = sorted(results, key=lambda item: -status.get("links", {})
                     .get(item[0], {}).get("score", 0.0))
    files = H.write_bundles([(url, verdict, [], []) for url, verdict in ordered], status)
    H.log(f"重建完成：{files} 个文件，耗时 {time.time() - started:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
