// Runs Aether's own providers library (from ref/) against a real title and records every request.
// Two setups: "native" = direct requests (like the libcurl page), "browser" = through the CORS proxies (like aether.cx).
import fs from "fs";
import path from "path";
globalThis.self = globalThis; globalThis.window = globalThis; globalThis.document = {};
const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const REF = path.join(ROOT, "ref", "aether.cx");
const out = (s) => process.stdout.write(s + "\n");
const map = JSON.parse(fs.readFileSync(path.join(REF, "providers.json"), "utf8"));
const mod = await import(path.join(REF, map.file));
const mk = mod[map.makeProviders], std = mod[map.makeStandardFetcher], tg = mod[map.targets], pxf = mod[map.proxyFetcher];
const PROXIES = (process.env.PROXIES || "https://just.raccoonunbutteredbagel.zip,https://justice.raccoonunbutteredbagel.zip").split(",").map((s) => s.trim()).filter(Boolean);
const TYPE = process.env.TYPE || "movie";
const MEDIA = TYPE === "movie"
  ? { type: "movie", title: process.env.TITLE || "Fight Club", releaseYear: Number(process.env.YEAR || 1999), tmdbId: process.env.TMDB || "550", imdbId: process.env.IMDB || "tt0137523" }
  : { type: "show", title: "Breaking Bad", releaseYear: 2008, tmdbId: "1396", imdbId: "tt0903747", season: { number: 1, tmdbId: "3572" }, episode: { number: 1, tmdbId: "62085" } };
const TIMEOUT = Number(process.env.TIMEOUT || 30000);
const UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

let current = null;
async function logged(u, init = {}) {
  const t0 = Date.now();
  const headers = new Headers(init.headers || {});
  if (!headers.has("user-agent")) headers.set("user-agent", UA);
  const entry = { url: String(u).slice(0, 180), method: init.method || "GET" };
  current && current.reqs.push(entry);
  try {
    const r = await fetch(u, { ...init, headers, signal: AbortSignal.timeout(20000) });
    entry.status = r.status; entry.ms = Date.now() - t0; entry.type = (r.headers.get("content-type") || "").split(";")[0];
    try { entry.body = (await r.clone().text()).replace(/\s+/g, " ").slice(0, 140); } catch {}
    return r;
  } catch (e) { entry.error = String(e && e.message || e).slice(0, 120); entry.ms = Date.now() - t0; throw e; }
}

function setups() {
  const std1 = std(logged);
  let pi = 0;
  const proxied = async (url, ops) => {
    let err;
    for (let i = 0; i < PROXIES.length; i++) {
      const p = PROXIES[(pi + i) % PROXIES.length];
      try { const r = await pxf(p, logged)(url, ops); pi = (pi + i) % PROXIES.length; return r; } catch (e) { err = e; }
    }
    throw err;
  };
  return {
    native: mk({ fetcher: std1, proxiedFetcher: std1, target: tg.NATIVE }),
    browser: mk({ fetcher: std1, proxiedFetcher: proxied, target: tg.BROWSER }),
  };
}

const withTimeout = (p, ms) => Promise.race([p, new Promise((_, rej) => setTimeout(() => rej(new Error("timed out after " + ms + "ms")), ms))]);
const result = { when: new Date().toISOString(), media: MEDIA, proxies: PROXIES, modes: {} };

for (const [mode, P] of Object.entries(setups())) {
  const rows = [];
  for (const s of P.listSources().filter((x) => x.mediaTypes.includes(MEDIA.type))) {
    current = { reqs: [] };
    const row = { id: s.id };
    try {
      const r = await withTimeout(P.runSourceScraper({ id: s.id, media: MEDIA }), TIMEOUT);
      row.ok = true;
      row.streams = (r.stream || []).map((st) => ({ type: st.type, url: String(st.playlist || (st.qualities && Object.values(st.qualities)[0]?.url) || "").slice(0, 120) }));
      row.embeds = (r.embeds || []).map((e) => e.embedId);
    } catch (e) {
      row.ok = false; row.error = e && e.constructor && e.constructor.name + ": " + String(e.message).slice(0, 160);
    }
    row.reqs = current.reqs;
    rows.push(row);
    out(`[${mode}] ${s.id}: ${row.ok ? "OK streams=" + row.streams.length + " embeds=" + row.embeds.length : row.error}  (${row.reqs.length} requests, last ${row.reqs.at(-1)?.status ?? row.reqs.at(-1)?.error ?? "-"})`);
  }
  result.modes[mode] = rows;
}
current = null;
fs.writeFileSync(path.join(ROOT, "ref", "_provider_test.json"), JSON.stringify(result, null, 1));
out("wrote ref/_provider_test.json");
process.exit(0);
