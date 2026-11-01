#!/usr/bin/env python3
"""抓取 Free V2rayU 站点的当日节点包（base64 订阅 txt），过滤假节点并做 TCP 存活探测。

用法:
    python3 fetch_freev2rayu.py [--stamp YYYYMMDD] [--out DIR] [--timeout 2.0]
                                [--workers 32] [--max-articles 3] [--no-probe]

产出（默认写到 ../out/）:
    freev2rayu-<STAMP>.txt          明文，一行一个节点
    freev2rayu-<STAMP>-base64.txt   v2rayN 可直接粘贴的 base64 整包

设计要点:
- 站点是"每日一篇文章"结构，文章页里挂着 /uploads/<年>/<月>/<序号>-<日期>.txt（整段 base64）。
  所以不硬编码日期，而是每次现抓首页 → 取最新文章 → 解析文章里挂的资源链接，跟随站点自动更新。
- 只在 4xx/5xx 或资源缺失时退化到"页面内直连节点 / 页面内 base64 块"。
- 全部标准库实现（本机 python3 无 requests）。
"""
from __future__ import annotations

import argparse
import base64
import binascii
import concurrent.futures as cf
import datetime as dt
import ipaddress
import json
import os
import re
import socket
import sys
import time
import urllib.parse
import urllib.request

SITE = "https://freev2rayu.github.io"
LIST_URL = SITE + "/free-nodes/"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
RUN_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT = os.path.join(RUN_DIR, "out")

PROTO_RE = re.compile(r"(vmess|vless|trojan|ss|ssr|hysteria2|hysteria|tuic)://[^\s\"'<>]{6,}")
LINK_RE = re.compile(r"[a-z0-9]+://[^\s\"'<>]+")
ALLOWED = {"vmess", "vless", "trojan", "ss", "ssr", "hysteria2", "hysteria", "tuic"}


def pick_nodes(text: str) -> list[str]:
    return [normalize(x) for x in LINK_RE.findall(text) if x.split("://", 1)[0].lower() in ALLOWED]


# ---- 系统代理可能劫持 urllib（2026-10-31 实测）------------------------------
# macOS 系统代理开启后，urllib 会自动走 127.0.0.1:<port>。若代理客户端挂着但节点
# 已死，CONNECT 会立刻被关闭，表现为 `EOF occurred in violation of protocol`
# ——而 curl 不读系统代理，于是出现「curl 通、Python 全挂」的假象。
# 这里与 harvest_lite.py 保持同一策略：先探测代理能否真的转发，不通就直连。
_OPENER: urllib.request.OpenerDirector | None = None
_PROXY_PROBE_URL = "https://api.github.com/"


def _proxy_works() -> bool:
    if os.environ.get("HARVEST_NO_PROXY"):
        return False
    try:
        request = urllib.request.Request(_PROXY_PROBE_URL, headers={"User-Agent": UA})
        with urllib.request.build_opener().open(request, timeout=4) as response:
            return response.status < 500
    except Exception:
        return False


def opener() -> urllib.request.OpenerDirector:
    global _OPENER
    if _OPENER is None:
        if _proxy_works():
            _OPENER = urllib.request.build_opener()
        else:
            _OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return _OPENER


def http_get(url: str, timeout: float = 20.0, attempts: int = 3) -> str:
    last: Exception | None = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
            with opener().open(req, timeout=timeout) as r:
                return r.read().decode("utf-8", "replace")
        except Exception as exc:  # 瞬时 SSL/超时重试
            last = exc
            if i + 1 < attempts:
                time.sleep(2 if i == 0 else 5)
    raise last if last else RuntimeError("http_get failed")


def find_articles(html: str, max_articles: int) -> list[str]:
    """首页里的文章链接，按出现顺序（站点把最新放最前），排除分页。"""
    out: list[str] = []
    for href in re.findall(r'href="([^"]+\.htm)"', html):
        if re.search(r"/page-\d+\.htm$", href):
            continue
        if "free-nodes" not in href:
            continue
        url = href if href.startswith("http") else SITE + (href if href.startswith("/") else "/free-nodes/" + href)
        if url not in out:
            out.append(url)
    return out[:max_articles]


def article_date(url: str) -> str:
    m = re.search(r"(20\d{2})-(\d{1,2})-(\d{1,2})", url)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return "-"


def resource_urls(html: str) -> list[str]:
    urls = []
    for u in re.findall(r'https://freev2rayu\.github\.io/uploads/[^\s"\'<>]+\.txt', html):
        if u not in urls:
            urls.append(u)
    return urls


def decode_b64_any(text: str) -> str:
    t = text.strip()
    if "://" in t:
        return t
    t = re.sub(r"\s+", "", t)
    t += "=" * (-len(t) % 4)
    for dec in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            s = dec(t).decode("utf-8", "replace")
            if "://" in s:
                return s
        except (binascii.Error, ValueError):
            continue
    return ""


def normalize(link: str) -> str:
    return link.strip().replace("&amp;", "&")


def host_port(link: str) -> tuple[str, int] | None:
    """尽量从任意协议链接里拿到 (host, port)。"""
    scheme = link.split("://", 1)[0].lower()
    if scheme == "vmess":
        body = link.split("://", 1)[1]
        try:
            body += "=" * (-len(body) % 4)
            cfg = json.loads(base64.b64decode(body).decode("utf-8", "replace"))
            return str(cfg.get("add", "")).strip("[]"), int(cfg.get("port", 0) or 0)
        except Exception:
            return None
    m = re.match(r"^[a-z0-9]+://(?:[^@/?#]*@)?(\[[0-9a-fA-F:]+\]|[^:/?#@]+):(\d{1,5})", link)
    if m:
        return m.group(1).strip("[]"), int(m.group(2))
    # ss:// 整段 base64 的老格式
    if scheme == "ss":
        body = link.split("://", 1)[1].split("#", 1)[0]
        dec = decode_b64_any(body)
        m = re.search(r"@(\[[0-9a-fA-F:]+\]|[^:/?#@]+):(\d{1,5})", dec or "")
        if m:
            return m.group(1).strip("[]"), int(m.group(2))
    return None


def is_bogus(link: str) -> bool:
    hp = host_port(link)
    if not hp:
        return True
    host, port = hp
    if not host or port <= 0 or port > 65535:
        return True
    try:
        ip = ipaddress.ip_address(host)
        if ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_multicast or ip.is_unspecified:
            return True
        if str(ip) in ("192.0.2.1", "192.0.2.2", "203.0.113.1"):  # 文档/示例地址
            return True
    except ValueError:
        low = host.lower()
        if "example" in low or low in ("localhost", "test.com") or low.endswith(".invalid"):
            return True
    return False


def tcp_alive(link: str, timeout: float) -> bool:
    hp = host_port(link)
    if not hp:
        return False
    host, port = hp
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stamp", default=dt.date.today().strftime("%Y%m%d"))
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--timeout", type=float, default=2.0)
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--max-articles", type=int, default=2)
    ap.add_argument("--no-probe", action="store_true")
    args = ap.parse_args()

    t0 = time.time()
    print(f"=== freev2rayu {args.stamp} ===", flush=True)
    try:
        listing = http_get(LIST_URL)
    except Exception as e:
        print(f"首页抓取失败: {e!r}")
        return 2

    articles = find_articles(listing, args.max_articles)
    if not articles:
        print("首页未解析到文章链接")
        return 2

    links: list[str] = []
    per_article = []
    for art in articles:
        try:
            page = http_get(art)
        except Exception as e:
            print(f"  文章抓取失败 {art}: {e!r}")
            continue
        res = resource_urls(page)
        got = 0
        for rurl in res:
            try:
                plain = decode_b64_any(http_get(rurl))
            except Exception as e:
                print(f"  资源抓取失败 {rurl}: {e!r}")
                continue
            block = pick_nodes(plain)
            links.extend(block)
            got += len(block)
        if not res:  # 退化：页面内直连节点
            block = pick_nodes(page)
            links.extend(block)
            got += len(block)
        per_article.append((article_date(art), len(res), got))
        print(f"  文章 {article_date(art)}: 资源 {len(res)} 个 → 节点 {got} 条")

    # 去重 + 去假
    uniq = list(dict.fromkeys(links))
    bogus = [l for l in uniq if is_bogus(l)]
    candidates = [l for l in uniq if not is_bogus(l)]
    print(f"原始 {len(links)} 条 → 去重 {len(uniq)} → 剔除假/内网 {len(bogus)} → 待探测 {len(candidates)}")

    alive: list[str] = []
    if args.no_probe:
        alive = candidates
    else:
        with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
            for link, ok in zip(candidates, ex.map(lambda l: tcp_alive(l, args.timeout), candidates)):
                if ok:
                    alive.append(link)
        print(f"TCP 存活 {len(alive)}/{len(candidates)}")

    # 协议分布
    dist: dict[str, int] = {}
    for l in alive:
        k = l.split("://")[0]
        dist[k] = dist.get(k, 0) + 1

    os.makedirs(args.out, exist_ok=True)
    plain_path = os.path.join(args.out, f"freev2rayu-{args.stamp}.txt")
    b64_path = os.path.join(args.out, f"freev2rayu-{args.stamp}-base64.txt")
    body = "\n".join(alive) + ("\n" if alive else "")
    with open(plain_path, "w", encoding="utf-8") as f:
        f.write(body)
    with open(b64_path, "w", encoding="utf-8") as f:
        f.write(base64.b64encode(body.encode()).decode() + "\n")

    newest = max((d for d, _, _ in per_article if d != "-"), default="-")
    print(f"站点最新文章日期: {newest}")
    print(f"协议分布: {', '.join(f'{k} {v}' for k, v in sorted(dist.items(), key=lambda x: -x[1])) or '无'}")
    print(f"耗时 {int(time.time() - t0)}s")
    print(f"输出: {plain_path}  ({len(alive)} 条)")
    print(f"      {b64_path}")
    return 0 if alive else 1


if __name__ == "__main__":
    sys.exit(main())
