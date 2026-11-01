#!/usr/bin/env python3
"""
harvest_lite.py — morpheusadam/v2ray-config 的 harvest.py 订阅侧复刻（纯标准库）。

实现与 SKILL.md / references/algorithm-details.md 逐条对齐的四个阶段，
用于让 agent 得到与 harvest.py 一致的结果：

    python3 harvest_lite.py subs search  发现新仓库，并入 rapo.txt
    python3 harvest_lite.py subs collect 读 README / 猜常见路径 / 扫文件树，产出 subs/candidates.txt
    python3 harvest_lite.py subs prove   抓取候选：分类、base64 解码、解析端点、TCP 抽测、打分 → subs/status.json
    python3 harvest_lite.py subs write   淘汰过期、分数降序重写 subs/all.txt（+ bundles）
    python3 harvest_lite.py subs run     等价 collect → prove → write（可 --search 前置）
    python3 harvest_lite.py daily        全流程（可 --skip-search）

与原版的关键差异（有意为之，不影响订阅结果口径）：
  - 不含 proxies 流水线（本技能只覆盖订阅节点）。
  - 不更新 README 块 / SVG / 徽章（原仓库的展示层）。
  - 默认不扫文件树（--tree 开启，原版同样默认关闭）。

环境变量：GITHUB_TOKEN / GH_TOKEN 可选，仅提高 API 限流。
原版出处：https://github.com/morpheusadam/v2ray-config/blob/main/harvest.py
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import math
import os
import random
import re
import socket
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent

REPOS_FILE = HERE / "rapo.txt"
SUBS_FILE = HERE / "subs" / "all.txt"
SUBS_CANDIDATES = HERE / "subs" / "candidates.txt"
SUBS_STATUS = HERE / "subs" / "status.json"
SUBS_RETIRED = HERE / "subs" / "retired.txt"
BUNDLE_DIR = HERE / "subs" / "bundles"
CACHE_DIR = HERE / ".cache"

USER_AGENT = "v2ray-config-harvest/2.0 (+https://github.com/morpheusadam/v2ray-config)"
PROBE_BYTES = 64 * 1024
MAX_STALE_DAYS = 12
FORMAT_VERSION = "v1"

# ---------------------------------------------------------------- 传输层
# macOS 的系统代理设置会一直留着，哪怕代理客户端已经退出。urllib 默认读取这些
# 设置，于是每个请求都挂在一个没人监听的端口上直到超时——而 curl 不读系统代理
# 设置，于是出现「curl 通、Python 不通」的怪象。这里先探测代理端口，连不上就直连。
_OPENER: urllib.request.OpenerDirector | None = None
# 探测代理能不能真的转发（端口能连不代表能通：v2rayN 挂着但节点全死了，
# 或者代理进程卡住时，TCP 连接会成功而请求永远没有响应）。
_PROXY_PROBE_URL = "https://api.github.com/"


def _proxy_works() -> bool:
    if os.environ.get("HARVEST_NO_PROXY"):
        return False
    try:
        request = urllib.request.Request(
            _PROXY_PROBE_URL, headers={"User-Agent": USER_AGENT})
        with urllib.request.build_opener().open(request, timeout=4) as response:
            return response.status < 500
    except Exception:
        return False


def opener() -> urllib.request.OpenerDirector:
    """返回 opener：系统代理实际不通时强制直连。"""
    global _OPENER
    if _OPENER is None:
        if _proxy_works():
            _OPENER = urllib.request.build_opener()
        else:
            _OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return _OPENER


# ================================================================ http 基础

@dataclass
class Response:
    url: str
    status: int
    body: bytes
    headers: dict

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300

    def text(self) -> str:
        return self.body.decode("utf-8", errors="ignore")


RAW_PREFIX = "https://raw.githubusercontent.com/"
# 国内直连 raw.githubusercontent.com 经常被阻断（SYN 无响应），
# 这里按实测延迟排序挂镜像；第一个成功后会被记住，后续直接走镜像，
# 避免每个请求都先白等一次 raw 超时。
GH_MIRRORS = (
    "https://gh-proxy.com/",
    "https://ghproxy.net/",
    "https://ghfast.top/",
)
_GOOD_MIRROR: str | None = None
_BAD_MIRRORS: set[str] = set()
_MIRROR_PREFIXES = GH_MIRRORS + ("https://cdn.jsdelivr.net/gh/",)


def _prefix_of(candidate: str) -> str | None:
    for prefix in _MIRROR_PREFIXES:
        if candidate.startswith(prefix):
            return prefix
    return None


def _mirror_urls(url: str) -> list[str]:
    """为 raw.githubusercontent.com 链接生成镜像候选（其他域名返回空）。"""
    if not url.startswith(RAW_PREFIX):
        return []
    candidates = [prefix + url for prefix in GH_MIRRORS]
    parts = url[len(RAW_PREFIX):].split("/", 3)
    if len(parts) == 4:  # user/repo/ref/path → jsdelivr 的 gh 形态
        owner, repo, ref, path = parts
        candidates.append(f"https://cdn.jsdelivr.net/gh/{owner}/{repo}@{ref}/{path}")
    candidates = [c for c in candidates if _prefix_of(c) not in _BAD_MIRRORS]
    if _GOOD_MIRROR:
        candidates.sort(key=lambda c: 0 if c.startswith(_GOOD_MIRROR) else 1)
    return candidates


def _http_get_once(url: str, *, max_bytes: int | None, timeout: int,
                   headers: dict | None) -> Response | None:
    request_headers = {"User-Agent": USER_AGENT, "Accept": "*/*"}
    if max_bytes:
        request_headers["Range"] = f"bytes=0-{max_bytes - 1}"
    if headers:
        request_headers.update(headers)
    request = urllib.request.Request(url, headers=request_headers)
    try:
        with opener().open(request, timeout=timeout) as response:
            body = response.read(max_bytes) if max_bytes else response.read()
            return Response(url, response.status, body, dict(response.headers))
    except urllib.error.HTTPError as e:
        try:
            body = e.read(max_bytes or 4096)
        except Exception:
            body = b""
        return Response(url, e.code, body, dict(e.headers or {}))
    except Exception:
        return None


def http_get(url: str, *, max_bytes: int | None = None, timeout: int = 25,
             headers: dict | None = None) -> Response | None:
    """永不抛异常的 GET；None 表示完全没有应答。

    raw.githubusercontent.com 直连不通时自动改走镜像，返回值里的 url
    仍然是原始地址，保证 status/candidates 记录口径不变。
    """
    global _GOOD_MIRROR
    # 国内直连 raw 常常不是"不通"而是"挂十几秒偶尔还能通"，先试直连等于每个
    # 请求都白等一次超时。所以 raw 链接一律镜像优先，直连只做兜底。
    order = _mirror_urls(url) + [url]

    fallback: Response | None = None
    for candidate in order:
        response = _http_get_once(candidate, max_bytes=max_bytes,
                                  timeout=timeout, headers=headers)
        if response is not None and 200 <= response.status < 300:
            if candidate != url:
                prefix = _prefix_of(candidate)
                if prefix:
                    _GOOD_MIRROR = prefix
                response.url = url
            return response
        if candidate != url and (response is None or response.status >= 500):
            prefix = _prefix_of(candidate)      # 镜像真挂了，本次会话不再试它
            if prefix:
                _BAD_MIRRORS.add(prefix)
        if fallback is None and response is not None:
            fallback = response
    return fallback


def github_api(path: str, *, timeout: int = 25) -> tuple[int, object]:
    headers = {"Accept": "application/vnd.github+json"}
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    response = http_get(f"https://api.github.com{path}", timeout=timeout, headers=headers)
    if response is None:
        return 0, None
    try:
        return response.status, json.loads(response.text())
    except Exception:
        return response.status, None


def has_token() -> bool:
    return bool(os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN"))


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def days_since(value: str | None) -> float | None:
    stamp = parse_iso(value)
    if stamp is None:
        return None
    return (datetime.now(timezone.utc) - stamp).total_seconds() / 86400.0


def log(message: str = "") -> None:
    print(message, flush=True)


# ================================================================ URL 规范化

URL_RE = re.compile(r"""https?://[^\s'"`<>\\[\](){}|,;]+""")
TRAILING_JUNK = ".,;:!?'\"`)]}>*_"

DEAD_HOSTS = (
    "shields.io", "img.shields.io", "badgen.net", "camo.githubusercontent.com",
    "user-images.githubusercontent.com", "private-user-images.githubusercontent.com",
    "avatars.githubusercontent.com", "opencollective.com", "t.me", "telegram.me",
    "telegram.org", "twitter.com", "x.com", "youtube.com", "youtu.be", "discord.gg",
    "discord.com", "instagram.com", "facebook.com", "linkedin.com", "reddit.com",
    "stackoverflow.com", "wikipedia.org", "google.com", "gstatic.com", "apache.org",
    "opensource.org", "creativecommons.org", "python.org", "pypi.org", "npmjs.com",
    "nodejs.org", "docker.com", "hub.docker.com", "letsencrypt.org", "cloudflare.com",
    "developer.mozilla.org", "gnu.org", "choosealicense.com", "api.github.com",
    "docs.github.com", "gitter.im", "patreon.com", "buymeacoffee.com", "ko-fi.com",
    "paypal.me", "star-history.com", "hits.dwyl.com", "visitor-badge.laobi.icu",
)

MEDIA_SUFFIXES = (
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".bmp", ".ico", ".mp4", ".webm",
    ".mp3", ".zip", ".tar", ".gz", ".7z", ".rar", ".exe", ".apk", ".dmg", ".deb", ".rpm",
    ".pdf", ".ttf", ".woff", ".woff2", ".css", ".dat", ".db",
)
CODE_SUFFIXES = (".py", ".go", ".rs", ".java", ".kt", ".sh", ".ps1", ".php", ".rb", ".md")

NOISE_PATH_HINTS = (
    "/blob/", "/tree/", "/issues", "/pulls", "/releases", "/actions", "/stargazers",
    "/watchers", "/network/", "/graphs/", "/wiki", "/security", "/discussions",
    "requirements.txt", "license", "changelog", "contributing", "code_of_conduct",
    ".github/", "/workflows/", "package.json", "go.mod", "cargo.toml",
)


def normalize_url(raw: str) -> str | None:
    """顺序与原版一致：github blob/raw 重写 → jsdelivr 重写 → 剥离 ?raw=true。"""
    url = (raw or "").strip().rstrip(TRAILING_JUNK)
    if not url or not url.lower().startswith(("http://", "https://")):
        return None
    lowered = url.lower()
    if "<" in url or ">" in url or "{" in url or "}" in url:
        return None
    if "your_username" in lowered or "your_repository" in lowered:
        return None
    if url.startswith("https://github.com/") and "/blob/" in url:
        url = url.replace("https://github.com/", "https://raw.githubusercontent.com/", 1)
        url = url.replace("/blob/", "/", 1)
    elif url.startswith("https://github.com/") and "/raw/" in url:
        url = url.replace("https://github.com/", "https://raw.githubusercontent.com/", 1)
        url = url.replace("/raw/", "/", 1)
    jsdelivr = re.match(r"^https://cdn\.jsdelivr\.net/gh/([^/@]+)/([^/@]+)@([^/]+)/(.+)$", url)
    if jsdelivr:
        user, repo, ref, path = jsdelivr.groups()
        url = f"https://raw.githubusercontent.com/{user}/{repo}/{ref}/{path}"
    url = url.split("?raw=true")[0]
    try:
        if not urllib.parse.urlsplit(url).hostname:
            return None
    except Exception:
        return None
    return url


def is_plausible_sub(url: str) -> bool:
    lowered = url.lower()
    split = urllib.parse.urlsplit(lowered)
    host = split.hostname or ""
    if any(host == h or host.endswith("." + h) for h in DEAD_HOSTS):
        return False
    if split.path.endswith(MEDIA_SUFFIXES) or split.path.endswith(CODE_SUFFIXES):
        return False
    if any(hint in lowered for hint in NOISE_PATH_HINTS):
        return False
    if host == "github.com" and lowered.count("/") <= 4:
        return False
    return True


def extract_urls(text: str) -> list[str]:
    found, seen = [], set()
    for match in URL_RE.findall(text or ""):
        url = normalize_url(match)
        if url and is_plausible_sub(url) and url not in seen:
            seen.add(url)
            found.append(url)
    return found


# ================================================================ 配置与端点

CONFIG_SCHEMES = (
    "vless://", "vmess://", "trojan://", "ss://", "ssr://", "hysteria://",
    "hysteria2://", "hy2://", "tuic://", "juicity://", "anytls://", "wireguard://",
    "socks://",
)
MODERN_MARKERS = ("reality", "security=tls", "hysteria2://", "hy2://", "tuic://",
                  "xtls", "vless://", "grpc", "ws")
CONFIG_URI_RE = re.compile(
    r"(?:" + "|".join(re.escape(s) for s in CONFIG_SCHEMES) + r")[^\s\"'<>]+"
)
B64_ALPHABET = re.compile(r"^[A-Za-z0-9+/=_\-]+$")


def count_configs(text: str) -> tuple[int, list[str]]:
    total, protocols = 0, []
    for scheme in CONFIG_SCHEMES:
        n = text.count(scheme)
        if n:
            total += n
            protocols.append(scheme[:-3])
    return total, protocols


def try_base64(text: str, min_len: int = 64) -> str | None:
    """截尾到 4 的倍数再解码，绝不补 =。"""
    compact = re.sub(r"\s+", "", text)
    if len(compact) < min_len or not B64_ALPHABET.match(compact):
        return None
    compact = compact[: len(compact) - (len(compact) % 4)]
    for decoder in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            return decoder(compact).decode("utf-8", errors="ignore")
        except (binascii.Error, ValueError):
            continue
    return None


def parse_endpoint(uri: str) -> tuple[str, int] | None:
    """解析出一个配置实际拨号的 host:port；解析不出返回 None。"""
    scheme, sep, rest = uri.strip().partition("://")
    if not sep:
        return None
    scheme = scheme.lower()
    if scheme == "vmess":
        decoded = try_base64(rest.split("#")[0], min_len=24)
        if not decoded:
            return None
        try:
            obj = json.loads(decoded)
        except Exception:
            return None
        host = str(obj.get("add") or obj.get("host") or "").strip()
        try:
            port = int(str(obj.get("port") or 0))
        except ValueError:
            return None
        return (host, port) if host and 0 < port < 65536 else None
    if scheme == "ssr":
        decoded = try_base64(rest.split("#")[0], min_len=16)
        if not decoded:
            return None
        parts = decoded.split(":")
        if len(parts) >= 2 and parts[1].isdigit():
            return parts[0], int(parts[1])
        return None
    body = rest.split("#")[0]
    if "@" not in body and scheme == "ss":
        decoded = try_base64(body, min_len=16)
        if not decoded or "@" not in decoded:
            return None
        body = decoded
    body = body.split("?")[0].split("/")[0]
    authority = body.rsplit("@", 1)[-1]
    bracketed = re.match(r"^\[([^\]]+)\]:(\d+)$", authority)
    if bracketed:
        return bracketed.group(1), int(bracketed.group(2))
    host, _, port = authority.rpartition(":")
    if not host or not port.isdigit():
        return None
    port_number = int(port)
    if not 0 < port_number < 65536:
        return None
    if not re.match(r"^[A-Za-z0-9._-]+$", host):
        return None
    return host, port_number


def endpoints_of(text: str, limit: int = 400) -> list[tuple[str, int]]:
    seen, ordered = set(), []
    for uri in CONFIG_URI_RE.findall(text):
        endpoint = parse_endpoint(uri)
        if endpoint and endpoint not in seen:
            seen.add(endpoint)
            ordered.append(endpoint)
            if len(ordered) >= limit:
                break
    return ordered


def tcp_latency(endpoint: tuple[str, int], timeout: float = 3.0) -> float | None:
    started = time.perf_counter()
    try:
        with socket.create_connection(endpoint, timeout=timeout):
            return (time.perf_counter() - started) * 1000.0
    except Exception:
        return None


# ================================================================ 分类

@dataclass
class Verdict:
    kind: str = "other"
    configs: int = 0
    protocols: list = field(default_factory=list)
    links: list = field(default_factory=list)
    http_status: int = 0
    bytes: int = 0
    last_modified: str = ""
    note: str = ""
    digest: str = ""
    body: str = ""
    markers: int = 0
    uris: list = field(default_factory=list)


def classify(response: Response | None) -> Verdict:
    if response is None:
        return Verdict(kind="dead", note="no response")
    if not response.ok and response.status != 206:
        return Verdict(kind="dead", http_status=response.status, note=f"HTTP {response.status}")
    text = response.text()
    verdict = Verdict(
        http_status=response.status,
        bytes=len(response.body),
        last_modified=response.headers.get("Last-Modified", "")
        or response.headers.get("last-modified", ""),
    )
    if not text.strip():
        verdict.kind = "empty"
        return verdict
    head = text[:2000].lower()
    if "<html" in head or "<!doctype html" in head:
        verdict.kind = "html"
        return verdict
    plain = text
    count, protocols = count_configs(text)
    if count == 0:
        decoded = try_base64(text)
        if decoded:
            count, protocols = count_configs(decoded)
            if count:
                verdict.note = "base64"
                plain = decoded
    if count >= 2:
        verdict.kind = "configs"
        verdict.configs = count
        verdict.protocols = protocols
        verdict.body = plain
        verdict.digest = hashlib.sha256(plain.encode("utf-8", "ignore")).hexdigest()[:16]
        return verdict
    if re.search(r"^\s*proxies:", text[:4000], re.M) or "proxy-groups:" in head:
        verdict.kind = "clash"
        return verdict
    links = extract_urls(text)
    if len(links) >= 3:
        verdict.kind = "catalog"
        verdict.links = links
        return verdict
    return verdict


# ================================================================ 打分与状态

SUB_WEIGHTS = {
    "reach": 0.34,      # 服务器可达性
    "fresh": 0.20,      # 最近变更
    "clean": 0.14,      # 去重复
    "speed": 0.12,      # 延迟——故意小（排序按延迟实测比随机更差）
    "volume": 0.12,     # 承载量
    "modern": 0.08,     # 现代协议占比
}


def speed_score(latency_ms: float | None) -> float:
    if latency_ms is None or latency_ms <= 0:
        return 0.0
    if latency_ms <= 60:
        return 1.0
    if latency_ms >= 2000:
        return 0.0
    return 1.0 - math.log(latency_ms / 60.0) / math.log(2000.0 / 60.0)


def freshness_score(days: float | None) -> float:
    if days is None:
        return 0.5
    if days <= 1:
        return 1.0
    if days >= MAX_STALE_DAYS:
        return 0.0
    return 1.0 - (days - 1) / (MAX_STALE_DAYS - 1)


def volume_score(configs: int) -> float:
    if configs <= 0:
        return 0.0
    return min(1.0, math.log10(1 + configs) / math.log10(301))


def score_sub(record: dict) -> float:
    parts = {
        "reach": record.get("reachRatio", 0.0),
        "speed": speed_score(record.get("medianLatencyMs")),
        "fresh": freshness_score(days_since(record.get("lastChanged"))),
        "volume": volume_score(record.get("configs", 0)),
        "clean": record.get("cleanRatio", 0.0),
        "modern": record.get("modernRatio", 0.0),
    }
    record["scoreParts"] = {k: round(v, 3) for k, v in parts.items()}
    return round(100.0 * sum(SUB_WEIGHTS[k] * v for k, v in parts.items()), 2)


def load_json(path: Path, default: dict) -> dict:
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                for key, value in default.items():
                    data.setdefault(key, value)
                return data
        except Exception:
            log(f"{path.name} is unreadable — starting a new one")
    return dict(default)


def save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=1, ensure_ascii=False), encoding="utf-8")


def append_retired(path: Path, entries: list[tuple[str, str]]) -> None:
    if not entries:
        return
    stamp = now_iso()
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    if not existing:
        existing = ("# Retired by harvest.py. Kept so a source that returns is recognised\n"
                    "# rather than rediscovered as a stranger. Format: date\treason\tvalue\n\n")
    lines = [f"{stamp}\t{reason}\t{value}" for value, reason in entries]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(existing.rstrip("\n") + "\n" + "\n".join(lines) + "\n", encoding="utf-8")


def prune_stale(records: dict, *, alive_key: str, change_key: str | None,
                max_days: int) -> list[tuple[str, str]]:
    retired: list[tuple[str, str]] = []
    for key, record in list(records.items()):
        watched = days_since(record.get("firstSeen"))
        if watched is not None and watched < max_days:
            continue
        since_alive = days_since(record.get(alive_key))
        if since_alive is None or since_alive > max_days:
            reason = (f"no configs for {since_alive:.0f}d" if since_alive is not None
                      else "never alive")
            retired.append((key, reason))
            records.pop(key, None)
            continue
        if change_key:
            since_change = days_since(record.get(change_key))
            if since_change is not None and since_change > max_days:
                retired.append((key, f"unchanged for {since_change:.0f}d"))
                records.pop(key, None)
    return retired


# ================================================================ 仓库读写

REPO_RE = re.compile(r"^https://github\.com/([^/]+)/([^/\s]+)/?$")


def read_repos() -> list[str]:
    if not REPOS_FILE.exists():
        return []
    repos = []
    for line in REPOS_FILE.read_text(encoding="utf-8").splitlines():
        match = REPO_RE.match(line.strip())
        if match:
            repos.append(f"{match.group(1)}/{match.group(2)}")
    return repos


def write_repos(repos: list[str]) -> None:
    unique = sorted(set(repos), key=str.lower)
    REPOS_FILE.write_text("\n".join(f"https://github.com/{r}" for r in unique) + "\n",
                          encoding="utf-8")


def read_catalog_links() -> list[str]:
    if not SUBS_FILE.exists():
        return []
    return [line.strip() for line in SUBS_FILE.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")]


# ================================================================ search

DEFAULT_QUERIES = (
    "v2ray config", "v2ray subscription", "vless subscription", "free v2ray configs",
    "xray config collector", "v2ray-configs", "vmess subscription",
    "v2ray collector telegram", "free vpn configs vless", "sing-box subscription free",
    "hysteria2 subscription", "reality config free",
)


def stage_search(args) -> int:
    queries = args.query or list(DEFAULT_QUERIES)
    have = set(read_repos())
    found: set[str] = set()
    pause = 2.5 if has_token() else 7.0
    log(f"searching GitHub: {len(queries)} queries x {args.pages} pages"
        f"{' (authenticated)' if has_token() else ' (unauthenticated, slow)'}")
    for query in queries:
        for page in range(1, args.pages + 1):
            status, data = github_api(
                f"/search/repositories?q={urllib.parse.quote(query)}"
                f"&sort=updated&order=desc&per_page=100&page={page}")
            if status == 403:
                log("  rate limited — waiting 60s")
                time.sleep(60)
                continue
            if status != 200 or not isinstance(data, dict):
                log(f"  {query!r} page {page}: HTTP {status}")
                break
            items = data.get("items") or []
            for item in items:
                if item.get("full_name"):
                    found.add(item["full_name"])
            log(f"  {query!r} page {page}: {len(items)} results, {len(found)} distinct")
            if len(items) < 100:
                break
            time.sleep(pause)
        time.sleep(pause)
    new = sorted(found - have, key=str.lower)
    if new:
        write_repos(list(have | found))
        log(f"{len(new)} new repositories added to {REPOS_FILE.name}")
        for name in new[:40]:
            log(f"  {name}")
        if len(new) > 40:
            log(f"  … and {len(new) - 40} more")
    else:
        log("no new repositories")
    return len(new)


# ================================================================ collect

README_NAMES = ("README.md", "README.MD", "readme.md", "Readme.md", "README",
                "README.txt", "README.rst")
COMMON_PATHS = (
    "sub.txt", "subs.txt", "all.txt", "config.txt", "configs.txt", "mix.txt",
    "all_configs.txt", "subscription.txt", "v2ray.txt", "vless.txt", "vmess.txt",
    "trojan.txt", "ss.txt", "sub/all.txt", "sub/sub.txt", "configs/all.txt",
    "Splitted-By-Protocol/vless.txt", "Config/all.txt", "output/all.txt",
)
TREE_NAME_HINTS = ("sub", "config", "all", "mix", "vless", "vmess", "trojan",
                   "shadowsocks", "ss", "hysteria", "hy2", "tuic", "reality", "merged",
                   "output", "result", "server")


def fetch_readme(repo: str, *, refresh: bool = False) -> str | None:
    cache = CACHE_DIR / "readme" / (repo.replace("/", "__") + ".md")
    if cache.exists() and not refresh:
        return cache.read_text(encoding="utf-8", errors="ignore")
    for name in README_NAMES:
        response = http_get(f"https://raw.githubusercontent.com/{repo}/HEAD/{name}", timeout=25)
        if response and response.ok and response.body.strip():
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(response.text(), encoding="utf-8")
            return response.text()
    return None


def probe_common_paths(repo: str) -> list[str]:
    found = []
    for path in COMMON_PATHS:
        url = f"https://raw.githubusercontent.com/{repo}/HEAD/{path}"
        response = http_get(url, max_bytes=4096, timeout=15)
        if response and (response.ok or response.status == 206) and response.body.strip():
            found.append(url)
    return found


def scan_tree(repo: str, *, budget: dict) -> list[str]:
    if budget["left"] <= 0:
        return []
    data = None
    ref = "HEAD"
    for candidate_ref in ("HEAD", "main", "master"):
        budget["left"] -= 1
        status, payload = github_api(f"/repos/{repo}/git/trees/{candidate_ref}?recursive=1")
        if status == 403:
            budget["left"] = 0
            budget["rate_limited"] = True
            return []
        if status == 200 and isinstance(payload, dict):
            data, ref = payload, candidate_ref
            break
        if budget["left"] <= 0:
            return []
    if data is None:
        return []
    candidates = []
    for entry in data.get("tree") or []:
        if entry.get("type") != "blob":
            continue
        path = entry.get("path", "")
        lowered = path.lower()
        name = lowered.rsplit("/", 1)[-1]
        if not (name.endswith(".txt") or "." not in name):
            continue
        if any(noise in lowered for noise in ("readme", "license", "requirements", ".github/")):
            continue
        if not any(hint in lowered for hint in TREE_NAME_HINTS):
            continue
        candidates.append(path)
    candidates.sort(key=lambda p: (p.count("/"), len(p)))
    if len(candidates) > 40:
        candidates = candidates[:40]
    return [f"https://raw.githubusercontent.com/{repo}/{ref}/{urllib.parse.quote(p)}"
            for p in candidates]


def load_candidates() -> dict[str, str]:
    if not SUBS_CANDIDATES.exists():
        return {}
    origin = {}
    for line in SUBS_CANDIDATES.read_text(encoding="utf-8").splitlines():
        if line.strip():
            url, _, repo = line.partition("\t")
            origin.setdefault(url, repo)
    return origin


def stage_collect(args) -> dict[str, str]:
    repos = read_repos()
    if args.limit:
        repos = repos[: args.limit]
    log(f"{len(repos)} repositories in {REPOS_FILE.name}")
    origin: dict[str, str] = {}
    for url in read_catalog_links():
        origin[url] = "(catalog)"
    without_readme: list[str] = []

    def one(repo: str) -> tuple[str, list[str], bool]:
        readme = fetch_readme(repo, refresh=args.refresh)
        if readme is None:
            return repo, [], False
        return repo, extract_urls(readme), True

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for index, (repo, links, had_readme) in enumerate(pool.map(one, repos), 1):
            if not had_readme:
                without_readme.append(repo)
                continue
            for url in links:
                origin.setdefault(url, repo)
            if index % 20 == 0 or index == len(repos):
                log(f"  [{index}/{len(repos)}] {len(origin)} candidates so far")

    if without_readme and not args.no_guess:
        log(f"guessing common paths for {len(without_readme)} repositories with no README")
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for repo, links in zip(without_readme, pool.map(probe_common_paths, without_readme)):
                for url in links:
                    origin.setdefault(url, repo)

    if args.tree:
        seen_repos = {r for r in origin.values()}
        empty = [r for r in repos if r not in seen_repos]
        budget = {"left": args.tree_budget, "rate_limited": False}
        log(f"scanning file trees for {len(empty)} repositories (API budget {args.tree_budget})")
        for repo in empty:
            for url in scan_tree(repo, budget=budget):
                origin.setdefault(url, repo)
            if budget["left"] <= 0:
                log("    tree budget spent"
                    + (" (GitHub rate limit — set GITHUB_TOKEN)" if budget["rate_limited"] else ""))
                break

    SUBS_CANDIDATES.parent.mkdir(parents=True, exist_ok=True)
    SUBS_CANDIDATES.write_text(
        "\n".join(f"{url}\t{repo}" for url, repo in origin.items()) + "\n", encoding="utf-8")
    log(f"{len(origin)} unique candidates written to {SUBS_CANDIDATES.name}")
    return origin


# ================================================================ prove

def stage_prove(args, origin: dict[str, str] | None = None) -> dict:
    origin = origin if origin is not None else load_candidates()
    if not origin:
        log("no candidates — run `collect` first")
        return load_json(SUBS_STATUS, {"generatedAt": "", "links": {}})
    status = load_json(SUBS_STATUS, {"generatedAt": "", "links": {}})
    stamp = now_iso()
    pending = [u for u in origin if not args.only_new or u not in status["links"]]
    log(f"proving {len(pending)} links, {args.samples} servers sampled from each")

    endpoint_owners: dict[tuple[str, int], set[str]] = {}

    def check(url: str) -> tuple[str, Verdict, list, list]:
        verdict = classify(http_get(url, max_bytes=args.probe_bytes, timeout=args.timeout))
        if verdict.kind != "configs":
            return url, verdict, [], []
        endpoints = endpoints_of(verdict.body)
        sample = (endpoints if len(endpoints) <= args.samples
                  else random.sample(endpoints, args.samples))
        lowered = verdict.body.lower()
        verdict.markers = sum(lowered.count(m) for m in MODERN_MARKERS)
        verdict.uris = CONFIG_URI_RE.findall(verdict.body)[:args.per_source]
        verdict.body = ""
        return url, verdict, endpoints, sample

    alive = 0
    catalog_links: list[str] = []
    results: list[tuple[str, Verdict, list, list]] = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for index, item in enumerate(pool.map(check, pending), 1):
            url, verdict, endpoints, _ = item
            results.append(item)
            if verdict.kind == "configs":
                alive += 1
                for endpoint in endpoints:
                    endpoint_owners.setdefault(endpoint, set()).add(url)
            elif verdict.kind == "catalog":
                catalog_links.extend(verdict.links)
            if index % 100 == 0 or index == len(pending):
                log(f"  [{index}/{len(pending)}] {alive} carrying configs")

    fresh = [u for u in dict.fromkeys(catalog_links)
             if u not in origin and u not in status["links"]]
    if fresh and not args.no_recurse:
        log(f"{len(fresh)} links found inside catalogs — proving those too")
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for index, item in enumerate(pool.map(check, fresh), 1):
                url, verdict, endpoints, _ = item
                origin.setdefault(url, "(catalog)")
                results.append(item)
                if verdict.kind == "configs":
                    alive += 1
                    for endpoint in endpoints:
                        endpoint_owners.setdefault(endpoint, set()).add(url)
                if index % 100 == 0 or index == len(fresh):
                    log(f"  [{index}/{len(fresh)}] {alive} carrying configs")

    to_probe: dict[tuple[str, int], None] = {}
    for _, verdict, _, sample in results:
        if verdict.kind == "configs":
            for endpoint in sample:
                to_probe[endpoint] = None
    log(f"probing {len(to_probe)} distinct servers over TCP")

    latency: dict[tuple[str, int], float | None] = {}
    endpoints_list = list(to_probe)
    with ThreadPoolExecutor(max_workers=args.probe_workers) as pool:
        for index, (endpoint, ms) in enumerate(
                zip(endpoints_list, pool.map(lambda e: tcp_latency(e, args.tcp_timeout),
                                             endpoints_list)), 1):
            latency[endpoint] = ms
            if index % 500 == 0 or index == len(endpoints_list):
                reached = sum(1 for v in latency.values() if v is not None)
                log(f"  [{index}/{len(endpoints_list)}] {reached} reachable")

    for url, verdict, endpoints, sample in results:
        record = status["links"].setdefault(url, {"firstSeen": stamp})
        record.update({
            "kind": verdict.kind,
            "configs": verdict.configs,
            "protocols": verdict.protocols,
            "httpStatus": verdict.http_status,
            "bytes": verdict.bytes,
            "lastChecked": stamp,
            "repo": origin.get(url, record.get("repo", "")),
        })
        if verdict.last_modified:
            record["lastModified"] = verdict.last_modified
        if verdict.kind != "configs":
            record["score"] = 0.0
            continue
        record["lastAlive"] = stamp
        if record.get("digest") != verdict.digest:
            record["lastChanged"] = stamp
            record["changes"] = record.get("changes", 0) + 1
        record["digest"] = verdict.digest
        measured = [latency[e] for e in sample if e in latency]
        reached = [ms for ms in measured if ms is not None]
        record["sampled"] = len(measured)
        record["reachRatio"] = round(len(reached) / len(measured), 3) if measured else 0.0
        record["medianLatencyMs"] = round(statistics.median(reached), 1) if reached else None
        self_clean = len(endpoints) / verdict.configs if verdict.configs else 0.0
        shared = [1.0 / len(endpoint_owners.get(e, {url})) for e in endpoints] or [0.0]
        record["cleanRatio"] = round(0.5 * min(1.0, self_clean) + 0.5 * (sum(shared) / len(shared)), 3)
        record["modernRatio"] = round(min(1.0, verdict.markers / max(1, verdict.configs)), 3)
        record["score"] = score_sub(record)

    status["generatedAt"] = stamp
    save_json(SUBS_STATUS, status)
    log(f"{alive} links carry configs. status written to {SUBS_STATUS.name}")
    if not args.no_bundles:
        write_bundles(results, status)
    return status


# ================================================================ bundles

BUNDLE_PROTOCOLS = {
    "vless": ("vless",),
    "vmess": ("vmess",),
    "trojan": ("trojan",),
    "shadowsocks": ("ss", "ssr"),
    "hysteria2": ("hysteria2", "hy2", "hysteria"),
    "tuic": ("tuic",),
    "wireguard": ("wireguard", "warp"),
}
BUNDLE_SIZES = {"mini": 100, "lite": 300, "medium": 1000}


def bundle_header(name: str, count: int, note: str) -> str:
    return (f"# format: {FORMAT_VERSION}\n"
            f"# {name} — {count} configs, generated {now_iso()} by harvest_lite\n"
            f"# {note}\n"
            f"# Built from sources this project fetched, decoded and scored itself.\n"
            f"# https://github.com/morpheusadam/v2ray-config\n")


def write_bundles(results: list, status: dict) -> int:
    links = status.get("links", {})
    ranked: list[tuple[float, str, str]] = []
    seen: set[tuple[str, int]] = set()
    for url, verdict, _, _ in sorted(
            results, key=lambda r: -links.get(r[0], {}).get("score", 0.0)):
        if verdict.kind != "configs":
            continue
        score = links.get(url, {}).get("score", 0.0)
        for uri in verdict.uris:
            endpoint = parse_endpoint(uri)
            if endpoint is None or endpoint in seen:
                continue
            seen.add(endpoint)
            ranked.append((score, uri.split("://", 1)[0].lower(), uri.strip()))
    if not ranked:
        log("no configs collected — no bundles written")
        return 0
    BUNDLE_DIR.mkdir(parents=True, exist_ok=True)
    written: list[tuple[str, int]] = []

    def publish(name: str, uris: list[str], note: str) -> None:
        if not uris:
            return
        text = "\n".join(uris)
        (BUNDLE_DIR / f"{name}.txt").write_text(
            bundle_header(name, len(uris), note) + "\n" + text + "\n", encoding="utf-8")
        (BUNDLE_DIR / f"{name}-base64.txt").write_text(
            base64.b64encode(text.encode()).decode(), encoding="utf-8")
        written.append((name, len(uris)))

    everything = [uri for _, _, uri in ranked]
    publish("all", everything, "Everything, best source first.")
    for name, size in BUNDLE_SIZES.items():
        publish(name, everything[:size], f"The {size} configs from the highest-scoring sources.")
    for name, schemes in BUNDLE_PROTOCOLS.items():
        publish(name, [uri for _, scheme, uri in ranked if scheme in schemes],
                f"{name} only, best source first.")
    publish("reality", [uri for _, _, uri in ranked if "reality" in uri.lower()],
            "VLESS+REALITY only — the protocol that survives active probing best.")
    publish("tls", [uri for _, _, uri in ranked
                    if "security=tls" in uri.lower() or "tls" in uri.lower()],
            "Configs declaring TLS.")
    publish("best", [uri for score, _, uri in ranked if score >= 85][:2000],
            "Only from sources scoring 85+. Fewer, and better. The app's default source.")
    index = ["# The subscription bundles this project publishes.",
             "#",
             f"# Generated {now_iso()}.",
             ""]
    for name, count in written:
        index.append(f"{name}.txt          # {count} configs")
    (BUNDLE_DIR / "index.txt").write_text("\n".join(index) + "\n", encoding="utf-8")
    total_files = len(written) * 2 + 1
    log(f"bundles: {total_files} files from {len(ranked)} unique configs "
        f"({', '.join(f'{n} {c}' for n, c in written[:6])}…)")
    return total_files


# ================================================================ write

def write_subs(args, status: dict | None = None) -> int:
    status = status or load_json(SUBS_STATUS, {"generatedAt": "", "links": {}})
    links = status.get("links", {})
    for record in links.values():
        if record.get("kind") == "configs":
            record["score"] = score_sub(record)

    retired = prune_stale(links, alive_key="lastAlive", change_key="lastChanged",
                          max_days=args.max_stale_days)
    if retired:
        append_retired(SUBS_RETIRED, retired)
        log(f"retired {len(retired)} links (nothing alive or changed in "
            f"{args.max_stale_days} days) — recorded in {SUBS_RETIRED.name}")

    alive = [(url, record) for url, record in links.items()
             if record.get("kind") == "configs"
             and record.get("configs", 0) >= args.min_configs]
    alive.sort(key=lambda item: -item[1].get("score", 0.0))

    header = [
        f"# format: {FORMAT_VERSION}",
        "# Subscription sources for Auto Mode",
        "#",
        "# Most clients want one link that returns servers, not a list of links. For that,",
        "# use subs/bundles/best.txt — built from the sources below that scored 85 or better.",
        "#",
        "# One link per line. Lines starting with # are ignored, so notes like this are fine.",
        "# Paste this whole file into the Auto Mode settings window, or point \"Import file\" at it.",
        "#",
        "# Maintained by harvest.py, which runs once a day on GitHub Actions. Every link here",
        "# was fetched and proved to carry configs, and a sample of its servers was dialled",
        "# over TCP. The order is that measurement, best first: how many of its servers",
        "# answered, how recently the file changed, how little of it is duplicated, how fast",
        "# it answered, how much it carries, and whether it uses protocols worth having.",
        "#",
        f"# Anything that stops answering, or stops changing, for {args.max_stale_days} days leaves this file",
        f"# and is recorded in {SUBS_RETIRED.name}.",
        "#",
        f"# {len(alive)} links, generated {now_iso()}. None of them are operated by this project.",
    ]
    SUBS_FILE.parent.mkdir(parents=True, exist_ok=True)
    SUBS_FILE.write_text("\n".join(header + [""] + [url for url, _ in alive]) + "\n",
                         encoding="utf-8")
    status["generatedAt"] = now_iso()
    save_json(SUBS_STATUS, status)
    log(f"subs/all.txt: {len(alive)} links, best score "
        f"{alive[0][1].get('score', 0) if alive else 0}")
    return len(alive)


# ================================================================ cli

def add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workers", type=int, default=16, help="concurrent HTTP fetches")
    parser.add_argument("--timeout", type=int, default=25, help="seconds per fetch")
    parser.add_argument("--probe-bytes", type=int, default=PROBE_BYTES,
                        help=f"bytes read from each candidate (default {PROBE_BYTES})")
    parser.add_argument("--max-stale-days", type=int, default=MAX_STALE_DAYS,
                        help=f"retirement age in days (default {MAX_STALE_DAYS})")
    parser.add_argument("--limit", type=int, default=0, help="stop after N items (testing)")


def add_collect_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--refresh", action="store_true", help="ignore the README cache")
    parser.add_argument("--no-guess", action="store_true", help="skip common-path guessing")
    parser.add_argument("--tree", action="store_true",
                        help="list files via the API when a README gives nothing")
    parser.add_argument("--tree-budget", type=int, default=50,
                        help="API calls the tree scan may spend")


def add_prove_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--only-new", action="store_true", help="skip links already on record")
    parser.add_argument("--no-recurse", action="store_true", help="do not follow catalogs")
    parser.add_argument("--samples", type=int, default=12,
                        help="servers dialled per subscription (default 12)")
    parser.add_argument("--probe-workers", type=int, default=64, help="concurrent TCP probes")
    parser.add_argument("--tcp-timeout", type=float, default=3.0, help="seconds per TCP probe")
    parser.add_argument("--per-source", type=int, default=600,
                        help="configs taken from any one source into the bundles")
    parser.add_argument("--no-bundles", action="store_true", help="skip publishing bundles/")


def add_write_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--min-configs", type=int, default=5,
                        help="ignore links carrying fewer configs (default 5)")


def add_search_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--query", action="append", help="repeatable; replaces the defaults")
    parser.add_argument("--pages", type=int, default=2, help="pages of 100 per query")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Maintain subs/all.txt (harvest.py 订阅侧复刻).",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    top = parser.add_subparsers(dest="area", required=True)
    subs = top.add_parser("subs", help="the subscription catalog").add_subparsers(
        dest="command", required=True)
    p = subs.add_parser("search", help="find new repositories on GitHub")
    add_common(p)
    add_search_flags(p)
    p = subs.add_parser("collect", help="read every repository, collect candidates")
    add_common(p)
    add_collect_flags(p)
    p = subs.add_parser("prove", help="fetch each candidate and measure its servers")
    add_common(p)
    add_prove_flags(p)
    p = subs.add_parser("write", help="rank, retire and rewrite subs/all.txt")
    add_common(p)
    add_write_flags(p)
    p = subs.add_parser("run", help="collect, prove and write")
    for add in (add_common, add_collect_flags, add_prove_flags, add_write_flags):
        add(p)
    p.add_argument("--search", action="store_true", help="look for new repositories first")
    p.add_argument("--query", action="append", help="repeatable; replaces the defaults")
    p.add_argument("--pages", type=int, default=2, help="pages of 100 per query")

    p = top.add_parser("daily", help="search + collect + prove + write (what CI runs)")
    for add in (add_common, add_collect_flags, add_prove_flags, add_write_flags,
                add_search_flags):
        add(p)
    p.add_argument("--skip-search", action="store_true")

    args = parser.parse_args(argv)
    started = time.time()

    if args.area == "subs":
        if args.command == "search":
            stage_search(args)
        elif args.command == "collect":
            stage_collect(args)
        elif args.command == "prove":
            stage_prove(args)
        elif args.command == "write":
            write_subs(args)
        elif args.command == "run":
            if getattr(args, "search", False):
                stage_search(args)
            origin = stage_collect(args)
            write_subs(args, stage_prove(args, origin=origin))
    elif args.area == "daily":
        if not args.skip_search:
            stage_search(args)
        origin = stage_collect(args)
        write_subs(args, stage_prove(args, origin=origin))

    log(f"\ndone in {time.time() - started:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())