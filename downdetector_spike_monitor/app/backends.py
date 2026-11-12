"""
Pluggable data backends. Both return the same `Snapshot` shape so the spike
detector and HA layer don't care where the numbers came from.

  EnterpriseBackend  — official Downdetector Enterprise API v2 (needs Client ID/Secret)
  UnofficialBackend  — scrapes the public downdetector.com/status/<slug>/ page, the same
                       technique as the `downdetector-api` npm package. No credentials,
                       but Cloudflare blocks it intermittently and the page format can
                       change without notice. Best-effort only.
"""
from __future__ import annotations

import html as _html
import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Optional, Protocol

import httpx

from .dd_client import DowndetectorClient, Company
from .watchlist import WatchEntry, Target, host_to_search_term

log = logging.getLogger("dd.backend")


@dataclass
class Snapshot:
    target: Target
    stats_24: list[int]            # 15-min buckets, newest last
    baseline_current: int
    dd_status: str = ""            # success | warning | danger | "" (unknown)
    extra: dict = field(default_factory=dict)


class Backend(Protocol):
    name: str
    def resolve(self, entry: WatchEntry) -> list[Target]: ...
    def poll(self, targets: list[Target]) -> list[Snapshot]: ...
    def search(self, name: str, country: str = "") -> list[dict]: ...


# ----------------------------------------------------------------------------
# Official Enterprise API
# ----------------------------------------------------------------------------
# Downdetector runs one site per country. The API's /sites endpoint lists them;
# this map is a fallback for resolving kind=country without an extra call.
COUNTRY_SITE_DOMAINS = {
    "US": "downdetector.com", "GB": "downdetector.co.uk", "CA": "downdetector.ca",
    "AU": "downdetector.com.au", "DE": "allestörungen.de", "FR": "downdetector.fr",
    "IT": "downdetector.it", "ES": "downdetector.es", "NL": "allestoringen.nl",
    "MX": "downdetector.mx", "BR": "downdetector.com.br", "IN": "downdetector.in",
    "JP": "downdetector.jp", "SE": "downdetector.se", "IE": "downdetector.ie",
}


class EnterpriseBackend:
    name = "enterprise"

    def __init__(self, client: DowndetectorClient):
        self.client = client
        self._sites: Optional[dict[str, int]] = None  # country_iso -> site_id

    def _site_for_country(self, iso: str) -> Optional[int]:
        if self._sites is None:
            self._sites = {}
            try:
                for s in self.client.list_sites():
                    iso_code = (s.get("country_iso") or s.get("country") or "").upper()
                    if iso_code and s.get("id") is not None:
                        self._sites[iso_code] = int(s["id"])
            except Exception as e:  # noqa: BLE001
                log.warning("could not list sites: %s", e)
        return self._sites.get(iso.upper())

    def _to_target(self, c: Company, entry: WatchEntry, city: Optional[str] = None) -> Target:
        return Target(company_id=c.id, name=c.name, slug=c.slug, country_iso=c.country_iso,
                      url=c.url, source_entry=entry.key(),
                      latitude=entry.latitude if city else None,
                      longitude=entry.longitude if city else None, city=city)

    def search(self, name: str, country: str = "") -> list[dict]:
        return [c.raw for c in self.client.search_companies(name=name, country=country)]

    def resolve(self, entry: WatchEntry) -> list[Target]:
        if entry.kind == "company":
            hits = self.client.search_companies(slug=entry.value, country=entry.country or "")
            hits = [h for h in hits if h.slug == entry.value] or hits[:1]
            return [self._to_target(h, entry) for h in hits]

        if entry.kind == "url":
            term = host_to_search_term(entry.value)
            hits = self.client.search_companies(name=term, country=entry.country or "")
            # prefer a company whose url host matches
            host = host_to_search_term(entry.value)
            exact = [h for h in hits if host_to_search_term(h.url or "") == host]
            return [self._to_target(h, entry) for h in (exact or hits[:1])]

        if entry.kind == "country":
            site_id = self._site_for_country(entry.value)
            if site_id is None:
                log.warning("no Downdetector site for country %s", entry.value)
                return []
            return [self._to_target(c, entry) for c in self.client.site_companies(site_id)]

        if entry.kind == "city":
            out = []
            for slug in entry.companies:
                hits = self.client.search_companies(slug=slug, country=entry.country or "")
                for h in hits[:1]:
                    out.append(self._to_target(h, entry, city=entry.value))
            return out
        return []

    def poll(self, targets: list[Target]) -> list[Snapshot]:
        snaps: list[Snapshot] = []
        plain = [t for t in targets if t.latitude is None]
        geo = [t for t in targets if t.latitude is not None]

        if plain:
            by_id = {c.id: c for c in self.client.get_companies({t.company_id for t in plain})}
            for t in plain:
                c = by_id.get(t.company_id)
                if c:
                    snaps.append(Snapshot(t, c.stats_24, c.baseline_current, c.status,
                                          {"stats_60": c.stats_60}))

        # City-scoped: reports endpoint filtered by lat/lon; baseline = median of the
        # local series since the API's baseline is country-wide.
        for t in geo:
            try:
                rows = self.client.get_reports([t.slug], interval="15m",
                                               latitude=t.latitude, longitude=t.longitude)
                series = [int(r.get("total", r.get("value", 0))) for r in rows][-96:]
                if series:
                    srt = sorted(series)
                    baseline = srt[len(srt) // 2]
                    snaps.append(Snapshot(t, series, baseline, "", {"scope": "city"}))
            except Exception as e:  # noqa: BLE001
                log.warning("city poll failed for %s: %s", t.uid, e)
        return snaps


# ----------------------------------------------------------------------------
# Unofficial scraper (no credentials)
# ----------------------------------------------------------------------------
# downdetector.com is a Next.js app. Each /status/<slug>/ page server-renders a
# React Server Components payload that embeds the same data the Enterprise API
# returns:  stats.status (success|warning|danger) and chartData.dataPoints[96]
# of {timestampUtc, reportsValue, baselineValue}.  Verified 2026-11-12.
#
# The payload is JSON-escaped inside a JS string, so quotes appear as \".
# Regexes below accept both the escaped and plain forms.
_Q = r'\\?"'                                   # optional backslash + quote
_POINT_RE = re.compile(
    _Q + r"timestampUtc" + _Q + r":" + _Q + r"([^\"\\]+)" + _Q + r"," +
    _Q + r"reportsValue" + _Q + r":(\d+)," + _Q + r"baselineValue" + _Q + r":(\d+)")
_STATUS_RE = re.compile(
    _Q + r"__typename" + _Q + r":" + _Q + r"CompanyStatsType" + _Q + r"[^{}]*?" +
    _Q + r"status" + _Q + r":" + _Q + r"(danger|warning|success)" + _Q)
_COMPANY_RE = re.compile(
    _Q + r"__typename" + _Q + r":" + _Q + r"CompanyType" + _Q + r"," +
    _Q + r"id" + _Q + r":" + _Q + r"(\d+)" + _Q + r"," + _Q + r"name" + _Q + r":" + _Q + r"([^\"\\]+)" + _Q)
_POGO_RE = re.compile(r"PogoConfig = (\{[^;]*\})")
_DIRECTORY_RE = re.compile(r'<a title="([^"]+)"[^>]*href="/status/([a-z0-9\-]+)/"')
_INDICATOR_RE = re.compile(_Q + r"localizedName" + _Q + r":" + _Q + r"([^\"\\]+)" + _Q + r"," + _Q + r"count" + _Q + r":(\d+)")
_CITY_RE = re.compile(_Q + r"__typename" + _Q + r":" + _Q + r"CityType" + _Q + r"[^{}]*?" + _Q + r"name" + _Q + r":" + _Q + r"([^\"\\]+)" + _Q)


class RateLimited(RuntimeError):
    pass


def _fetch_html(url: str, timeout: float = 25.0) -> str:
    """Fetch with a real-Chrome TLS fingerprint (curl_cffi) if available, else httpx.
    Cloudflare sits in front of downdetector.com; the browser-like fingerprint is
    what usually gets a plain client through without a JS challenge."""
    try:
        from curl_cffi import requests as cffi_requests  # type: ignore
        r = cffi_requests.get(url, impersonate="chrome", timeout=timeout)
        status, text = r.status_code, r.text
    except ImportError:
        r = httpx.get(url, timeout=timeout, follow_redirects=True,
                      headers={"User-Agent": UnofficialBackend.UA, "Accept-Language": "en-US,en"})
        status, text = r.status_code, r.text
    if status == 429:
        raise RateLimited(f"{url} -> 429: rate limited by Downdetector/Cloudflare")
    if status == 403 or "challenge-platform" in text[:20000]:
        raise RuntimeError(f"{url} -> {status}: Cloudflare challenge (install curl_cffi, "
                           "lower poll rate, or set FLARESOLVERR_URL)")
    if status == 404:
        raise RuntimeError(f"{url} -> 404: unknown slug (check /companies/ directory)")
    if status != 200:
        raise RuntimeError(f"{url} -> {status}")
    return text


def _fetch_via_flaresolverr(url: str, solver_url: str, timeout: float = 60.0) -> str:
    """Optional: FlareSolverr (https://github.com/FlareSolverr/FlareSolverr) runs a real
    browser and solves Cloudflare challenges. Set FLARESOLVERR_URL=http://host:8191/v1."""
    r = httpx.post(solver_url, json={"cmd": "request.get", "url": url, "maxTimeout": int(timeout * 1000)},
                   timeout=timeout + 5)
    body = r.json()
    if body.get("status") != "ok":
        raise RuntimeError(f"flaresolverr: {body.get('message')}")
    return body["solution"]["response"]


class UnofficialBackend:
    name = "unofficial"
    UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
    DIRECTORY_TTL = 24 * 3600

    def __init__(self, flaresolverr_url: str = "", fetch=None, request_spacing: float = 3.0,
                 backoff: float = 30.0):
        self.flaresolverr_url = flaresolverr_url
        self.request_spacing = request_spacing   # seconds between page fetches
        self.backoff = backoff                   # wait before the single retry after a 429
        self._fetch = fetch                      # injectable for tests
        self._dir: dict[str, tuple[float, dict[str, str]]] = {}   # domain -> (ts, {slug: name})

    # ------------------------------------------------------------- fetching
    def _domain(self, country: Optional[str]) -> str:
        return COUNTRY_SITE_DOMAINS.get((country or "US").upper(), "downdetector.com")

    def fetch(self, url: str) -> str:
        if self._fetch:
            return self._fetch(url)
        if self.flaresolverr_url:
            return _fetch_via_flaresolverr(url, self.flaresolverr_url)
        return _fetch_html(url)

    def directory(self, country: Optional[str]) -> dict[str, str]:
        """{slug: name} for every company on that country's site (cached 24h)."""
        dom = self._domain(country)
        ts, cached = self._dir.get(dom, (0.0, {}))
        if cached and time.time() - ts < self.DIRECTORY_TTL:
            return cached
        html = self.fetch(f"https://{dom}/companies/")
        d = {slug: _html.unescape(name) for name, slug in _DIRECTORY_RE.findall(html)}
        if not d:
            log.warning("company directory parsed 0 entries from %s (layout changed?)", dom)
        self._dir[dom] = (time.time(), d)
        return d

    # -------------------------------------------------------------- parsing
    @staticmethod
    def parse_page(html: str) -> dict:
        pts = _POINT_RE.findall(html)
        m_status = _STATUS_RE.search(html)
        m_co = _COMPANY_RE.search(html)
        m_pogo = _POGO_RE.search(html)
        pogo = {}
        if m_pogo:
            try:
                pogo = json.loads(m_pogo.group(1))
            except json.JSONDecodeError:
                pass
        return {
            "timestamps": [t for t, _, _ in pts],
            "reports": [int(r) for _, r, _ in pts],
            "baseline": [int(b) for _, _, b in pts],
            "status": m_status.group(1) if m_status else ("danger" if pogo.get("outage") else ""),
            "company_id": int(m_co.group(1)) if m_co else 0,
            "name": _html.unescape(m_co.group(2)) if m_co else "",
            "category": pogo.get("category", ""),
            "indicators": [{"name": n, "count": int(c)} for n, c in _INDICATOR_RE.findall(html)],
            "cities": _CITY_RE.findall(html),
        }

    # ------------------------------------------------------------- resolve
    def search(self, name: str, country: str = "") -> list[dict]:
        q = name.lower()
        d = self.directory(country)
        hits = [(s, n) for s, n in d.items() if q in n.lower() or q in s]
        hits.sort(key=lambda x: (not x[0].startswith(q), len(x[1])))
        return [{"id": 0, "name": n, "slug": s, "country_iso": (country or "US").upper(),
                 "url": f"https://{self._domain(country)}/status/{s}/"} for s, n in hits[:25]]

    def _target(self, slug: str, name: str, entry: WatchEntry, city: Optional[str] = None) -> Target:
        return Target(company_id=0, name=name or slug, slug=slug,
                      country_iso=(entry.country or "US").upper(),
                      url=f"https://{self._domain(entry.country)}/status/{slug}/",
                      source_entry=entry.key(), city=city)

    def resolve(self, entry: WatchEntry) -> list[Target]:
        d = self.directory(entry.country)
        if entry.kind == "company":
            if entry.value in d:
                return [self._target(entry.value, d[entry.value], entry)]
            hits = self.search(entry.value, entry.country or "")
            if not hits:
                log.warning("no company matching %r on %s", entry.value, self._domain(entry.country))
                return []
            return [self._target(hits[0]["slug"], hits[0]["name"], entry)]
        if entry.kind == "url":
            term = host_to_search_term(entry.value)
            hits = self.search(term, entry.country or "")
            exact = [h for h in hits if h["slug"] == term or h["name"].lower() == term]
            pick = (exact or hits)[:1]
            return [self._target(h["slug"], h["name"], entry) for h in pick]
        if entry.kind == "country":
            log.warning("country=%s resolves to %d companies; one page fetch each per poll "
                        "— raise poll_interval accordingly", entry.value, len(d))
            return [self._target(s, n, entry) for s, n in d.items()]
        if entry.kind == "city":
            log.warning("unofficial backend has no geo filter; watching %s nationally", entry.companies)
            return [self._target(s, d.get(s, s), entry) for s in entry.companies]
        return []

    # ---------------------------------------------------------------- poll
    def poll(self, targets: list[Target]) -> list[Snapshot]:
        snaps = []
        for i, t in enumerate(targets):
            if i and self.request_spacing:
                time.sleep(self.request_spacing)
            try:
                try:
                    page = self.fetch(t.url)
                except RateLimited:
                    log.info("429 for %s; backing off %.0fs and retrying once", t.slug, self.backoff)
                    time.sleep(self.backoff)
                    page = self.fetch(t.url)
                data = self.parse_page(page)
                if not data["reports"]:
                    log.warning("no chart points parsed for %s (page layout changed?)", t.slug)
                    continue
                if data["company_id"] and not t.company_id:
                    t.company_id = data["company_id"]
                if data["name"]:
                    t.name = data["name"]
                snaps.append(Snapshot(t, data["reports"][-96:], data["baseline"][-1],
                                      data["status"], {"category": data["category"],
                                                       "last_point": data["timestamps"][-1],
                                                       "indicators": data["indicators"],
                                                       "cities": data["cities"]}))
            except Exception as e:  # noqa: BLE001
                log.warning("unofficial poll failed for %s: %s", t.slug, e)
        return snaps
