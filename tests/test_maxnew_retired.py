#!/usr/bin/env python3
"""离线验证 2026-11-26 新增的两条防超时逻辑（不联网、不碰真实数据）。

1) retired_recently()  —— collect 阶段跳过近期退役的源
2) cap_new_links()     —— prove 阶段限制每轮新链接数
3) 两者的集成路径（stage_collect / stage_prove 实际调用）
"""
import sys
import tempfile
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import harvest_lite as H

FAIL = []


def check(name, cond, detail=""):
    print(("✅ " if cond else "❌ ") + name + (f"  {detail}" if detail else ""))
    if not cond:
        FAIL.append(name)


def test_retired_recently():
    tmp = Path(tempfile.mkdtemp()) / "retired.txt"
    tmp.write_text(
        "# header\n"
        "2026-11-24T00:00:00Z\tno configs for 15d\thttps://x/recent.txt\n"
        "2026-08-01T00:00:00Z\tnever alive\thttps://x/old.txt\n"
        "malformed-line-without-tabs\n",
        encoding="utf-8",
    )
    got = H.retired_recently(30, tmp)
    check("retired_recently 抓到近期退役项", got == {"https://x/recent.txt"}, str(got))
    check("retired_recently 忽略过期退役项", "https://x/old.txt" not in got)
    check("retired_recently 窗口为 0 时禁用", H.retired_recently(0, tmp) == set())


def test_cap_new_links():
    items = [f"u{i}" for i in range(10)]
    check("cap_new_links 截断", H.cap_new_links(items, 3) == items[:3])
    check("cap_new_links 0 表示不限", H.cap_new_links(items, 0) == items)
    check("cap_new_links 上限大于总数时原样返回", H.cap_new_links(items, 99) == items)


def test_collect_filter():
    tmp = Path(tempfile.mkdtemp())
    retired = tmp / "retired.txt"
    retired.write_text("2026-11-25T00:00:00Z\tno configs for 14d\thttps://raw.githubusercontent.com/demo/repo/main/dead.txt\n",
                       encoding="utf-8")
    candidates = tmp / "candidates.txt"

    H.read_repos = lambda: ["demo/repo"]
    H.read_catalog_links = lambda: []
    H.fetch_readme = lambda repo, refresh=False: (
        "https://raw.githubusercontent.com/demo/repo/main/live.txt\n"
        "https://raw.githubusercontent.com/demo/repo/main/dead.txt\n"
    )
    H.SUBS_CANDIDATES = candidates
    H.SUBS_RETIRED = retired

    args = types.SimpleNamespace(limit=0, refresh=False, no_guess=True, tree=False,
                                 tree_budget=0, workers=2, retired_days=30)
    origin = H.stage_collect(args)
    written = candidates.read_text(encoding="utf-8")
    check("collect 过滤掉近期退役的源", "dead.txt" not in origin and "dead.txt" not in written,
          f"origin={list(origin)}")
    check("collect 保留正常源", any("live.txt" in u for u in origin), f"origin={list(origin)}")

    args0 = types.SimpleNamespace(**{**args.__dict__, "retired_days": 0})
    origin0 = H.stage_collect(args0)
    check("retired_days=0 时不过滤", any("dead.txt" in u for u in origin0))


def test_prove_cap():
    tmp = Path(tempfile.mkdtemp())
    cand = tmp / "candidates.txt"
    cand.write_text("\n".join(f"https://example.org/{i}.txt\t(dead)" for i in range(5)) + "\n",
                    encoding="utf-8")
    status = tmp / "status.json"
    H.SUBS_CANDIDATES = cand
    H.SUBS_STATUS = status
    H.http_get = lambda *a, **k: None          # 全部判 dead，不触网

    args = types.SimpleNamespace(only_new=False, max_new=2, samples=12, workers=4,
                                 timeout=8, probe_bytes=65536, tcp_timeout=2.0,
                                 probe_workers=4, per_source=600, no_recurse=True,
                                 no_bundles=True, max_stale_days=14)
    H.stage_prove(args)
    import json
    written = json.loads(status.read_text(encoding="utf-8"))["links"]
    check("prove 只处理 max_new 条", len(written) == 2, f"实际 {len(written)} 条")


if __name__ == "__main__":
    test_retired_recently()
    test_cap_new_links()
    test_collect_filter()
    test_prove_cap()
    print()
    print("全部通过" if not FAIL else f"失败 {len(FAIL)} 项: {FAIL}")
    sys.exit(1 if FAIL else 0)
