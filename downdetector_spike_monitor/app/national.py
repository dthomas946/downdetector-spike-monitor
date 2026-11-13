"""
National scan: one fetch of a country's Downdetector homepage per run.

The homepage server-renders `topCompanies`: the services with the most problem
reports in that country right now (47 on downdetector.com), each with
Downdetector's status (success | warning | danger) and a 96-point / 24 h
report sparkline. Verified against the live site 2026-11-12.

From that we derive a "widespread outage" signal: several services at
`danger` with thousands of reports in the last hour. When it fires we also
check Cloudflare's public status API, since a Cloudflare incident is the most
common single cause of many unrelated sites failing at once.
"""
from __future__ import annotations

import html as _html
import logging
import re
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

import httpx

log = logging.getLogger("dd.national")

CLOUDFLARE_STATUS_URL = "https://www.cloudflarestatus.com/api/v2/summary.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ------------------------------------------------------------------ parsing
_FIELD = {
    "id": re.compile(r'^\s*,\s*"id":"(\d+)"'),
    "name": re.compile(r'"name":"((?:[^"\\]|\\.)*)"'),
    "slug": re.compile(r'"slug":"([a-z0-9\-]+)"'),
    "category": re.compile(r'"category":\{"__typename":"CategoryType","slug":"([a-z0-9\-]+)"'),
    "status": re.compile(r'"CompanyStatsType"[^\[]*?"status":"(danger|warning|success)"'),
    "sparkline": re.compile(r'"sparkline":\[([\d,\s]*)\]'),
    "calc": re.compile(r'"stats24CalculatedAt":"([^"]+)"'),
    "corr": re.compile(r'"correlationOutage":(null|\{.{0,400}?\})'),
}


def parse_homepage(html: str) -> list[dict]:
    """Return the homepage's topCompanies as dicts, in Downdetector's rank order."""
    t = html.replace('\\"', '"')
    out, seen = [], set()
    for seg in t.split('"__typename":"CompanyType"')[1:]:
        seg = seg[:6000]
        m_spark = _FIELD["sparkline"].search(seg)
        m_slug = _FIELD["slug"].search(seg)
        if not (m_spark and m_slug):
            continue
        slug = m_slug.group(1)
        if slug in seen:
            continue
        seen.add(slug)
        spark = [int(x) for x in m_spark.group(1).split(",") if x.strip()]
        get = lambda k: (_FIELD[k].search(seg).group(1) if _FIELD[k].search(seg) else "")
        corr = get("corr")
        out.append({
            "rank": len(out) + 1,
            "id": int(get("id") or 0),
            "name": _html.unescape(get("name")),
            "slug": slug,
            "category": get("category"),
            "status": get("status"),
            "sparkline": spark,
            "current": spark[-1] if spark else 0,
            "last_hour_max": max(spark[-4:]) if spark else 0,
            "peak_24h": max(spark) if spark else 0,
            "baseline_window": estimate_baseline(spark),
            "baseline_est": estimate_baseline(spark),
            "calculated_at": get("calc"),
            "correlation": None if corr in ("", "null") else corr[:400],
        })
    return out


def estimate_baseline(spark: list[int]) -> int:
    """The homepage has no baseline series. Estimate 'normal' as the 20th percentile of
    the 24 h window, excluding the most recent 3 h: an outage that fills up to ~80% of
    the window doesn't drag it up (the median did). The monitor additionally keeps the
    lowest estimate seen over 7 days, so a day-long outage can't become 'normal'."""
    hist = sorted(spark[:-12] if len(spark) > 24 else spark)
    if not hist:
        return 1
    return max(1, int(hist[int(0.2 * (len(hist) - 1))]))


# --------------------------------------------------------------- cloudflare
def check_cloudflare(timeout: float = 15.0, get: Optional[Callable] = None) -> dict:
    """Summarise Cloudflare's public Statuspage (no Cloudflare challenge on it)."""
    try:
        if get:
            body = get(CLOUDFLARE_STATUS_URL)
        else:
            r = httpx.get(CLOUDFLARE_STATUS_URL, timeout=timeout)
            r.raise_for_status()
            body = r.json()
        status = body.get("status", {})
        incidents = [{"name": i.get("name"), "status": i.get("status"), "impact": i.get("impact"),
                      "url": i.get("shortlink"), "started": i.get("started_at") or i.get("created_at")}
                     for i in body.get("incidents", [])]
        degraded = [c.get("name") for c in body.get("components", [])
                    if c.get("status") not in (None, "operational") and not c.get("group")]
        indicator = status.get("indicator", "unknown")          # none | minor | major | critical
        return {
            "checked_at": _now_iso(),
            "indicator": indicator,
            "description": status.get("description", ""),
            "incidents": incidents,
            "degraded_components": degraded[:25],
            "degraded_count": len(degraded),
            "likely_cause": indicator in ("major", "critical") or any(
                i.get("impact") in ("major", "critical") for i in incidents),
        }
    except Exception as e:  # noqa: BLE001
        log.warning("Cloudflare status check failed: %s", e)
        return {"checked_at": _now_iso(), "indicator": "unknown", "error": str(e)}


# ----------------------------------------------------------------- scanner
@dataclass
class WidespreadConfig:
    min_services: int = 3          # this many services ...
    min_reports: int = 1000        # ... each with >= this many reports in the last hour ...
    require_danger: bool = True    # ... and Downdetector status == danger
    check_cloudflare: bool = True


@dataclass
class NationalState:
    country: str
    scanned_at: Optional[str] = None
    error: Optional[str] = None
    companies: list[dict] = field(default_factory=list)
    widespread: bool = False
    widespread_since: Optional[str] = None
    widespread_cleared_at: Optional[str] = None
    affected: list[dict] = field(default_factory=list)
    cloudflare: Optional[dict] = None
    scans: int = 0

    def summary(self) -> dict:
        danger = [c for c in self.companies if c["status"] == "danger"]
        top = self.companies[0] if self.companies else None
        return {
            "country": self.country,
            "scanned_at": self.scanned_at,
            "error": self.error,
            "widespread": self.widespread,
            "widespread_since": self.widespread_since,
            "widespread_cleared_at": self.widespread_cleared_at,
            "services_listed": len(self.companies),
            "services_in_danger": len(danger),
            "total_reports_15m": sum(c["current"] for c in self.companies),
            "top_service": top["name"] if top else None,
            "top_service_reports": top["current"] if top else 0,
            "affected": [{k: c.get(k) for k in ("name", "slug", "category", "status", "current",
                                                 "last_hour_max", "baseline_est", "severity_level",
                                                 "severity_score")} for c in self.affected],
            "cloudflare": self.cloudflare,
        }


def evaluate_widespread(cfg: WidespreadConfig, companies: list[dict]) -> list[dict]:
    return [c for c in companies
            if c["last_hour_max"] >= cfg.min_reports
            and (c["status"] == "danger" or not cfg.require_danger)]


def next_scan_time(minute: int, now: Optional[float] = None) -> float:
    """Epoch seconds of the next HH:<minute>:00 (local clock of the container = UTC)."""
    now = now or time.time()
    t = time.gmtime(now)
    candidate = now - (t.tm_min * 60 + t.tm_sec) + minute * 60
    return candidate if candidate > now + 1 else candidate + 3600
