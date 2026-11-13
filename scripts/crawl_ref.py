#!/usr/bin/env python3
"""Crawl an app into ref/<host>/, locate every backend request it makes, and probe those backends.

All requests go out with aether.cx-style browser headers (Origin / Referer / Sec-Fetch-* / Accept per type).
Outputs next to the crawled files: _manifest.json, _requests.json (endpoints found in code), _probes.json
(live status of each backend route). A readable summary goes to the Actions job summary.
"""
import gzip, hashlib, json, os, re, sys, time, urllib.error, urllib.parse, urllib.request, zlib
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REF = os.path.join(ROOT, "ref")
UA = os.environ.get("CRAWL_UA") or "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
MAX_FILES = int(os.environ.get("MAX_FILES") or "2000")
WORKERS = int(os.environ.get("WORKERS") or "8")

CRAWL_URL = (os.environ.get("CRAWL_URL") or "https://aether.cx").strip()
_u = urllib.parse.urlparse(CRAWL_URL)
if _u.scheme not in ("http", "https") or not _u.netloc:
    sys.exit(f"CRAWL_URL must be a full http(s) URL, got {CRAWL_URL!r}")
APP_ORIGIN = f"{_u.scheme}://{_u.netloc}"
OUT = os.path.join(REF, _u.netloc)
split = lambda s: [o.strip().rstrip("/") for o in (s or "").split(",") if o.strip()]
ORIGINS = [APP_ORIGIN] + split(os.environ.get("EXTRA_ORIGINS"))
EXTRA_PROBE = split(os.environ.get("PROBE_ORIGINS"))
inside = lambda u: any(u.startswith(o + "/") or u == o for o in ORIGINS)

SEEDS = ["/manifest.webmanifest", "/manifest.json", "/site.webmanifest", "/sw.js", "/service-worker.js",
         "/robots.txt", "/sitemap.xml", "/config.js", "/ping.txt", "/browserconfig.xml", "/404.html"]
PROBE_PATHS = ["/", "/metadata", "/api/metadata", "/v1/metadata", "/sources", "/api/sources", "/scrape",
               "/scrape/source", "/scrape/embed", "/ping", "/health", "/status", "/version", "/openapi.json", "/api"]

EXT_KIND = {}
for k, exts in {"html": "html htm", "js": "js mjs cjs", "css": "css", "json": "json map webmanifest",
                "img": "png jpg jpeg gif webp svg ico avif", "font": "woff woff2 ttf otf", "media": "mp3 ogg wav mp4 webm"}.items():
    for e in exts.split():
        EXT_KIND[e] = k
ACCEPT = {"html": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
          "css": "text/css,*/*;q=0.1", "img": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
          "json": "application/json,text/plain,*/*", "font": "*/*", "media": "*/*", "js": "*/*"}
DEST = {"js": "script", "css": "style", "img": "image", "font": "font", "media": "video", "json": "empty"}

def kind_of(url):
    p = urllib.parse.urlparse(url).path
    if p.endswith("/") or p == "":
        return "html"
    return EXT_KIND.get(os.path.splitext(p)[1].lstrip(".").lower(), "html" if "." not in os.path.basename(p) else "other")

def headers_for(url, accept=None):
    kind = kind_of(url)
    doc = kind == "html"
    same = urllib.parse.urlparse(url).netloc == _u.netloc
    h = {"User-Agent": UA, "Accept": accept or ACCEPT.get(kind, "*/*"), "Accept-Language": "en-US,en;q=0.9",
         "Accept-Encoding": "gzip, deflate", "Referer": APP_ORIGIN + "/", "Sec-Fetch-Site": "same-origin" if same else "cross-site"}
    if doc and same:
        h.update({"Sec-Fetch-Mode": "navigate", "Sec-Fetch-Dest": "document", "Upgrade-Insecure-Requests": "1"})
    else:
        h.update({"Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": DEST.get(kind, "empty"), "Origin": APP_ORIGIN})
    return h

def decode(data, enc):
    enc = (enc or "").lower()
    try:
        if "gzip" in enc:
            return gzip.decompress(data)
        if "deflate" in enc:
            try:
                return zlib.decompress(data)
            except zlib.error:
                return zlib.decompress(data, -zlib.MAX_WBITS)
    except Exception:
        pass
    return data

def http(url, accept=None, limit=None, timeout=45, tries=3):
    h = headers_for(url, accept)
    if limit:
        h["Accept-Encoding"] = "identity"
    last = None
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=timeout) as r:
                data = r.read(limit) if limit else r.read()
                return {"status": r.status, "url": r.geturl(), "type": r.headers.get("Content-Type", ""),
                        "data": decode(data, r.headers.get("Content-Encoding")), "headers": dict(r.headers)}
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and i < tries - 1:
                time.sleep(1.5 * (i + 1))
                continue
            try:
                body = e.read(limit or 65536)
            except Exception:
                body = b""
            return {"status": e.code, "url": url, "type": e.headers.get("Content-Type", ""),
                    "data": decode(body, e.headers.get("Content-Encoding")), "headers": dict(e.headers)}
        except Exception as e:
            last = e
            time.sleep(1.0 * (i + 1))
    return {"status": 0, "url": url, "type": "", "data": b"", "headers": {}, "error": str(last)}

class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.urls = []
    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        for k in ("src", "href", "data-src", "data-href", "poster", "content"):
            v = a.get(k)
            if v and (k != "content" or v.startswith(("/", "./", "http"))):
                self.urls.append(v)
        if a.get("srcset"):
            self.urls += [p.strip().split()[0] for p in a["srcset"].split(",") if p.strip()]

ASSET_EXT = r"(?:js|mjs|cjs|css|json|wasm|data|mem|png|jpe?g|gif|webp|avif|svg|ico|woff2?|ttf|otf|mp3|ogg|wav|mp4|webm|txt|map|bin|pck|unityweb|br|gz|webmanifest|html)"
STR_RE = re.compile(r"""["'`]((?:\.{0,2}/)?[\w\-./@%~+]+\.""" + ASSET_EXT + r""")(?:\?[^"'`\s]*)?["'`]""")
CSS_URL = re.compile(r"url\(\s*['\"]?([^'\")]+)['\"]?\s*\)")
CSS_IMP = re.compile(r"@import\s+['\"]([^'\"]+)['\"]")
SRCMAP = re.compile(r"sourceMappingURL=([^\s*]+)")
DYN_IMPORT = re.compile(r"""import\(\s*["'`]([^"'`]+)["'`]\s*\)""")

# ---- request location -------------------------------------------------------------------------
URL_RE = re.compile(r"""https?://[A-Za-z0-9\-._~:/?#\[\]@!$&'()*+,;=%{}]+?(?=["'`\s<>\\)]|$)""")
CONF_RE = re.compile(r"""([A-Z][A-Z0-9_]*(?:URL|URLS|HOST|ORIGIN|BACKEND|API|PROXY)[A-Z0-9_]*)["']?\s*[:=]\s*["'`](https?://[^"'`]+)["'`]""")
ROUTE_RE = re.compile(r"""["'`](/[A-Za-z0-9_\-]+(?:/[A-Za-z0-9_\-.:{}$]+)*)/?(?:\?[^"'`\s]*)?["'`]""")
TPL_ROUTE_RE = re.compile(r"""\$\{[^}]{1,60}\}(/[A-Za-z0-9_\-]+(?:/[A-Za-z0-9_\-.:{}$]+)*)""")
CONCAT_RE = re.compile(r"""[\w$\])]\s*\+\s*["'`](/[A-Za-z0-9_\-]+(?:/[A-Za-z0-9_\-.:{}$]+)*)["'`]""")
CALL_RE = re.compile(r"""(fetch|EventSource|XMLHttpRequest|ofetch|axios(?:\.\w+)?|\.open|sendBeacon|WebSocket)\s*\(\s*([^)]{0,160})""")
NOISE_HOSTS = ("w3.org", "schema.org", "mozilla.org", "reactjs.org", "react.dev", "github.com", "githubusercontent.com",
               "googleapis.com", "gstatic.com", "google.com", "googletagmanager.com", "google-analytics.com", "cloudflare.com",
               "discord.gg", "discord.com", "xmlns", "example.com", "localhost", "apple.com", "duck.com", "ietf.org", "whatwg.org")
NOISE_ROUTE = re.compile(r"^/(?:[a-z]{2}(?:-[A-Za-z]{2})?|\d+|[a-z])/?$")

def find_requests(text, file):
    r = {"urls": {}, "config": {}, "routes": {}, "calls": []}
    for m in URL_RE.finditer(text):
        u = m.group(0).rstrip(".,;")
        host = urllib.parse.urlparse(u).netloc.lower()
        if host and not any(n in host for n in NOISE_HOSTS):
            r["urls"].setdefault(u, 0)
            r["urls"][u] += 1
    for k, v in CONF_RE.findall(text):
        r["config"].setdefault(k, set()).add(v)
    for rx in (ROUTE_RE, TPL_ROUTE_RE, CONCAT_RE):
        for m in rx.finditer(text):
            route = m.group(1)
            if len(route) < 3 or NOISE_ROUTE.match(route) or re.search(r"\.(?:js|css|png|svg|jpg|webp|ico|woff2?|json|html|map)$", route):
                continue
            r["routes"].setdefault(route, 0)
            r["routes"][route] += 1
    for m in CALL_RE.finditer(text):
        r["calls"].append({"file": file, "call": m.group(1), "args": re.sub(r"\s+", " ", m.group(2))[:140]})
    return r

def merge_requests(acc, r):
    for k in ("urls", "routes"):
        for a, n in r[k].items():
            acc[k][a] = acc[k].get(a, 0) + n
    for k, vs in r["config"].items():
        acc["config"].setdefault(k, set()).update(vs)
    acc["calls"] += r["calls"]

# ---- crawl ------------------------------------------------------------------------------------
def local_path(url, ctype=""):
    p = urllib.parse.urlparse(url)
    path = p.path
    if path.endswith("/") or path == "":
        path += "index.html"
    elif "html" in ctype.lower() and not os.path.splitext(path)[1]:
        path += "/index.html"
    path = path.lstrip("/")
    if p.query:
        stem, ext = os.path.splitext(path)
        path = f"{stem}__{hashlib.md5(p.query.encode()).hexdigest()[:8]}{ext}"
    return os.path.normpath(os.path.join(REF, p.netloc, path))

def strip(u):
    p = urllib.parse.urlparse(u)
    qs = [(k, v) for k, v in urllib.parse.parse_qsl(p.query) if k != "token"]
    return urllib.parse.urlunparse(p._replace(query=urllib.parse.urlencode(qs), fragment=""))

def candidates(final, f):
    """A reference may be relative to the file or (bundlers) to the site root, so queue both."""
    out = [urllib.parse.urljoin(final, f)]
    if not f.startswith(("/", "./", "../", "http")) and "/" in f:
        out.append(urllib.parse.urljoin(APP_ORIGIN + "/", f))
    return out

def crawl(start_url):
    seen, manifest, missing = set(), [], []
    reqs = {"urls": {}, "config": {}, "routes": {}, "calls": []}
    q = deque([start_url] + [APP_ORIGIN + s for s in SEEDS])
    first = True
    with ThreadPoolExecutor(WORKERS) as pool:
        while q and len(manifest) < MAX_FILES:
            batch = []
            while q and len(batch) < WORKERS * 4:
                url = q.popleft()
                key = strip(url)
                if key not in seen:
                    seen.add(key)
                    batch.append((url, key))
            for (url, key), res in zip(batch, pool.map(lambda b: http(b[0]), batch)):
                final = res["url"]
                if res["status"] != 200 or not inside(final):
                    if res["status"] and res["status"] != 200 and inside(url):
                        missing.append({"url": key, "status": res["status"]})
                    elif not res["status"]:
                        print(f"  skip {key}: {res.get('error')}", file=sys.stderr)
                    continue
                data, ctype = res["data"], res["type"]
                dest = local_path(strip(final) if urllib.parse.urlparse(final).path not in ("", "/") or not first else key, ctype)
                first = False
                if not dest.startswith(REF):
                    continue
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                with open(dest, "wb") as fh:
                    fh.write(data)
                rel = os.path.relpath(dest, ROOT)
                manifest.append({"url": key, "file": rel, "type": ctype, "bytes": len(data)})
                print(f"  saved {key} ({len(data)} B)")
                c = ctype.lower()
                if not (any(t in c for t in ("html", "javascript", "css", "json", "text", "xml")) or dest.endswith((".js", ".mjs", ".css", ".html", ".json", ".map", ".webmanifest"))):
                    continue
                text = data.decode("utf-8", "ignore")
                merge_requests(reqs, find_requests(text, rel))
                found = []
                if "html" in c:
                    lp = Links()
                    lp.feed(text)
                    found += lp.urls
                found += CSS_URL.findall(text) + CSS_IMP.findall(text) + STR_RE.findall(text) + SRCMAP.findall(text) + DYN_IMPORT.findall(text)
                for f in found:
                    if f.startswith(("data:", "blob:", "javascript:", "mailto:", "#")):
                        continue
                    for nu in candidates(final, f):
                        if inside(nu) and strip(nu) not in seen:
                            q.append(nu)
    return manifest, missing, reqs

# ---- probing ----------------------------------------------------------------------------------
def probe(origin, path):
    res = http(origin + path, accept="application/json,text/event-stream,*/*", limit=2048, timeout=20, tries=2)
    body = res["data"].decode("utf-8", "ignore")
    return {"origin": origin, "path": path, "status": res["status"], "type": res["type"],
            "x_token": "X-Token" in res["headers"] or "x-token" in {k.lower() for k in res["headers"]},
            "body": re.sub(r"\s+", " ", body)[:300], **({"error": res["error"]} if res.get("error") else {})}

def backend_origins(reqs):
    out = set(EXTRA_PROBE)
    for vs in reqs["config"].values():
        for v in vs:
            for part in v.split(","):
                p = urllib.parse.urlparse(part.strip())
                if p.scheme and p.netloc and p.netloc != _u.netloc and not any(n in p.netloc for n in NOISE_HOSTS):
                    out.add(f"{p.scheme}://{p.netloc}")
    for u in reqs["urls"]:
        p = urllib.parse.urlparse(u)
        if any(w in p.netloc for w in ("backend", "scrape")) and not any(n in p.netloc for n in NOISE_HOSTS):
            out.add(f"{p.scheme}://{p.netloc}")
    return sorted(out)

def run_probes(reqs):
    origins = backend_origins(reqs)
    routes = sorted(reqs["routes"], key=lambda r: -reqs["routes"][r])[:8]
    paths = list(dict.fromkeys(PROBE_PATHS + routes))
    jobs = [(o, p) for o in origins for p in paths]
    print(f"probing {len(origins)} origin(s) x {len(paths)} path(s)")
    with ThreadPoolExecutor(WORKERS) as pool:
        return origins, list(pool.map(lambda j: probe(*j), jobs))

def summary(manifest, missing, reqs, origins, probes):
    L = [f"## Crawl of {CRAWL_URL}", f"- saved **{len(manifest)}** files, **{len(missing)}** missing/blocked", ""]
    L.append("### Backend origins found")
    L += [f"- `{o}`" for o in origins] or ["- none"]
    L += ["", "### Config URLs in code"]
    L += [f"- `{k}` = {', '.join('`'+v+'`' for v in sorted(vs))}" for k, vs in sorted(reqs["config"].items())] or ["- none"]
    L += ["", "### Probe results (non-404 first)"]
    for p in sorted(probes, key=lambda p: (p["status"] == 404, p["origin"], p["path"])):
        L.append(f"- `{p['status']}` {p['origin']}{p['path']}" + (f" - {p['body'][:100]}" if p["body"] else ""))
    L += ["", "### Most-referenced routes in code"]
    L += [f"- `{r}` x{n}" for r, n in sorted(reqs["routes"].items(), key=lambda x: -x[1])[:40]]
    return "\n".join(L)

def jsonable(reqs):
    return {"urls": dict(sorted(reqs["urls"].items(), key=lambda x: -x[1])),
            "config": {k: sorted(v) for k, v in sorted(reqs["config"].items())},
            "routes": dict(sorted(reqs["routes"].items(), key=lambda x: -x[1])),
            "calls": reqs["calls"][:400]}

if __name__ == "__main__":
    print(f"crawling {CRAWL_URL} (origin {APP_ORIGIN}, {WORKERS} workers)")
    os.makedirs(OUT, exist_ok=True)
    manifest, missing, reqs = crawl(CRAWL_URL)
    if not manifest:
        sys.exit(f"nothing saved: could not fetch {CRAWL_URL} (check the URL/hostname)")
    origins, probes = run_probes(reqs)
    dump = lambda name, obj: open(os.path.join(OUT, name), "w").write(json.dumps(obj, indent=2))
    dump("_manifest.json", {"origin": APP_ORIGIN, "start": CRAWL_URL, "files": manifest, "missing": missing})
    dump("_requests.json", jsonable(reqs))
    dump("_probes.json", {"origins": origins, "results": probes})
    md = summary(manifest, missing, reqs, origins, probes)
    print(md)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        open(os.environ["GITHUB_STEP_SUMMARY"], "a").write(md + "\n")
    print(f"done: {len(manifest)} files -> ref/")
