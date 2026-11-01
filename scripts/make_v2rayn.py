#!/usr/bin/env python3
"""把 harvest_lite 的 bundles 转成 v2rayN 能直接吃的一版。

产出（写入 <bundles>/../out/）：
  v2rayN-<YYYYMMDD>-best.txt          纯分享链接，一行一条（全部高分源节点）
  v2rayN-<YYYYMMDD>-best-base64.txt   上面整份的 base64 单行（剪贴板导入用）
  v2rayN-<YYYYMMDD>-mini.txt          精选 100 条
  v2rayN-<YYYYMMDD>-mini-base64.txt
  v2rayN-<YYYYMMDD>-sources.txt       订阅源 URL 列表（在 v2rayN 里做在线订阅）

用法：
  python3 make_v2rayn.py <bundles 目录> [输出目录]
"""
from __future__ import annotations

import base64
import ipaddress
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from harvest_lite import parse_endpoint  # noqa: E402  复用同一套端点解析

SCHEMES = (
    "vless", "vmess", "trojan", "ss", "ssr", "hysteria", "hysteria2",
    "hy2", "tuic", "juicity", "anytls", "wireguard", "socks",
)
URI_RE = re.compile(r"^(%s)://\S+$" % "|".join(SCHEMES), re.IGNORECASE)

# README 里的示例行会带这些假地址，导入客户端只会白占位置
FAKE_HOSTS = {"localhost", "example.com", "example.org", "your.server.com",
              "server.com", "host.com", "ip", "host", "your-host"}


def is_fake_endpoint(uri: str) -> bool:
    """端点落在回环/内网/保留地址，或明显是文档占位，判为假节点。"""
    ep = parse_endpoint(uri)
    if ep is None:
        return False  # 解析不出来就别乱杀，交给客户端判断
    host = ep[0].strip("[]").lower()
    if host in FAKE_HOSTS:
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False  # 正常域名
    return ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_link_local


def read_configs(path: Path) -> tuple[list[str], int]:
    """读一个 bundle 文件，返回（去重后的分享链接, 过滤掉的假节点数）。"""
    if not path.exists():
        return [], 0
    seen: set[str] = set()
    out: list[str] = []
    dropped = 0
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if not URI_RE.match(line):
            continue
        if line in seen:
            continue
        seen.add(line)
        if is_fake_endpoint(line):
            dropped += 1
            continue
        out.append(line)
    return out, dropped


def write_bundle(outdir: Path, stem: str, configs: list[str]) -> dict:
    """写一份明文 + 一份 base64。返回统计信息。"""
    plain = outdir / f"{stem}.txt"
    b64 = outdir / f"{stem}-base64.txt"
    body = "\n".join(configs) + "\n"
    plain.write_text(body, encoding="utf-8")
    # v2rayN 剪贴板 / 订阅链接通用形态：整份 base64，单行无换行
    b64.write_text(base64.b64encode(body.encode()).decode(), encoding="utf-8")
    counts = Counter(c.split("://", 1)[0].lower() for c in configs)
    return {
        "stem": stem,
        "count": len(configs),
        "plain_bytes": plain.stat().st_size,
        "b64_bytes": b64.stat().st_size,
        "protocols": counts,
    }


def build_mini(configs: list[str], limit: int = 100, per_host: int = 2) -> list[str]:
    """跨协议轮询 + 同主机限流地取样。

    上游 bundles/mini.txt 是按来源分数顺序取的，最高分那个源恰好是单协议，
    100 条会被它一个占满 —— 那一个源失效，精选包就整包报废。
    这里改成按协议轮询、同一 host 最多 per_host 条，保证多点冗余。
    """
    buckets: dict[str, list[str]] = {}
    for cfg in configs:
        buckets.setdefault(cfg.split("://", 1)[0].lower(), []).append(cfg)

    order = sorted(buckets, key=lambda s: -len(buckets[s]))
    cursor = {s: 0 for s in order}
    host_count: Counter = Counter()
    picked: list[str] = []
    while len(picked) < limit:
        progressed = False
        for s in order:
            if len(picked) >= limit:
                break
            items = buckets[s]
            while cursor[s] < len(items):
                cfg = items[cursor[s]]
                cursor[s] += 1
                ep = parse_endpoint(cfg)
                host = ep[0].strip("[]").lower() if ep else "?"
                if host_count[host] >= per_host:
                    continue
                host_count[host] += 1
                picked.append(cfg)
                progressed = True
                break
        if not progressed:
            break
    return picked


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 2
    bundles = Path(argv[1]).expanduser().resolve()
    outdir = Path(argv[2]).expanduser().resolve() if len(argv) > 2 else bundles.parent / "out"
    if not bundles.is_dir():
        print(f"bundles 目录不存在: {bundles}", file=sys.stderr)
        return 1
    outdir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d")
    report: list[dict] = []
    dropped_total = 0

    best, d = read_configs(bundles / "best.txt")
    dropped_total += d
    if not best:
        best, d = read_configs(bundles / "medium.txt")
        if not best:
            best, d = read_configs(bundles / "all.txt")
        dropped_total += d
    if best:
        report.append(write_bundle(outdir, f"v2rayN-{stamp}-best", best))

    mini = build_mini(best, 100) if best else read_configs(bundles / "mini.txt")[0]
    if mini:
        report.append(write_bundle(outdir, f"v2rayN-{stamp}-mini", mini))

    # 订阅源 URL：从 all.txt 抽出裸链接（v2rayN 可直接当在线订阅添加）
    sources = outdir / f"v2rayN-{stamp}-sources.txt"
    src_count = 0
    alltxt = bundles.parent / "all.txt"
    if alltxt.exists():
        urls = [l.strip() for l in alltxt.read_text(encoding="utf-8", errors="replace").splitlines()
                if l.strip().startswith(("http://", "https://"))]
        sources.write_text("\n".join(urls) + "\n", encoding="utf-8")
        src_count = len(urls)

    print(f"== v2rayN 导出 {stamp} ==")
    for r in report:
        protos = ", ".join(f"{k} {v}" for k, v in r["protocols"].most_common())
        print(f"  {r['stem']}.txt  {r['count']} 条  "
              f"明文 {r['plain_bytes']//1024}KB / base64 {r['b64_bytes']//1024}KB  [{protos}]")
    print(f"  v2rayN-{stamp}-sources.txt  {src_count} 个订阅源 URL")
    if dropped_total:
        print(f"  已过滤 {dropped_total} 条示例/内网假节点（127.x、10.x、example.com 等）")
    print(f"输出目录: {outdir}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
