#!/usr/bin/env python3
"""Find the providers library inside a crawled Aether build and write ref/<host>/providers.json.

Aether bundles its scraper (makeProviders / standard fetcher / targets / simple-proxy fetcher) in a vendor chunk and
imports it into the app bundle under short names. We read the app bundle to learn which vendor exports those are.
"""
import json, os, re, sys, glob, urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HOST = urllib.parse.urlparse(os.environ.get("CRAWL_URL") or "https://aether.cx").netloc
BASE = os.path.join(ROOT, "ref", HOST)

def derive(app_js):
    # Ns(){... pa({fetcher:ga(fetch),proxiedFetcher:Af(),target:xa.BROWSER}) ...}
    m = re.search(r"(\w+)\(\{fetcher:(\w+)\(fetch\),proxiedFetcher:(\w+)\(\),target:(\w+)\.BROWSER\}\)", app_js)
    if not m:
        raise SystemExit("could not find the providers call in the app bundle")
    make, std, proxied, targets = m.groups()
    # Af(){return async(s,n)=>Fu(Nc(),Lf)(s,n)}
    m2 = re.search(r"function %s\(\)\{return async\(\w+,\w+\)=>(\w+)\(" % re.escape(proxied), app_js)
    if not m2:
        raise SystemExit("could not find the simple proxy fetcher")
    proxy_fetcher = m2.group(1)
    imp = re.search(r"import\{([^}]*\bas %s\b[^}]*)\}from\"\./(vendor-[^\"]+\.js)\"" % re.escape(make), app_js)
    if not imp:
        raise SystemExit("could not find the vendor import")
    names, vendor = imp.groups()
    alias = {}
    for part in names.split(","):
        a, _, b = part.partition(" as ")
        alias[(b or a).strip()] = a.strip()
    out = {"file": "assets/" + vendor}
    for key, local in (("makeProviders", make), ("makeStandardFetcher", std), ("targets", targets), ("proxyFetcher", proxy_fetcher)):
        if local not in alias:
            raise SystemExit(f"{key} ({local}) is not imported from the vendor chunk")
        out[key] = alias[local]
    return out

if __name__ == "__main__":
    app = sorted(glob.glob(os.path.join(BASE, "assets", "index-*.js")), key=os.path.getsize)[-1]
    # the main app bundle is the one that mentions the providers call
    for cand in sorted(glob.glob(os.path.join(BASE, "assets", "index-*.js")), key=os.path.getsize, reverse=True):
        if "runSourceScraper" in open(cand, encoding="utf8").read():
            app = cand
            break
    res = derive(open(app, encoding="utf8").read())
    res["app"] = "assets/" + os.path.basename(app)
    path = os.path.join(BASE, "providers.json")
    json.dump(res, open(path, "w"), indent=2)
    print("wrote", os.path.relpath(path, ROOT), res)
