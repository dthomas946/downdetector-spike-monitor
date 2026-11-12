"""
Watchlist: what to monitor. Stored as YAML; editable via the REST API.

Entry kinds
  company  — a Downdetector company by slug (e.g. "aws", "att"), optional country
  url      — a website URL; resolved to a company by searching its host name
  country  — every company on that country's Downdetector site (e.g. "US" -> downdetector.com)
  city     — a company (or all watched companies) filtered to reports near a lat/lon

Resolution turns each entry into one or more concrete `Target`s with a company id.
"""
from __future__ import annotations

import logging
import re
import threading
from pathlib import Path
from typing import Literal, Optional
from urllib.parse import urlparse

import yaml
from pydantic import BaseModel, Field

log = logging.getLogger("dd.watchlist")


class WatchEntry(BaseModel):
    kind: Literal["company", "url", "country", "city"]
    value: str                                   # slug | url | ISO country | city name
    country: Optional[str] = None                # ISO-2, narrows company/url search
    latitude: Optional[float] = None             # for kind=city
    longitude: Optional[float] = None
    radius_km: Optional[float] = 50              # informational; API filters by lat/lon
    companies: list[str] = Field(default_factory=list)  # for kind=city: slugs to watch there
    label: Optional[str] = None
    enabled: bool = True

    def key(self) -> str:
        return f"{self.kind}:{self.value}:{self.country or ''}"


class Target(BaseModel):
    """A concrete thing to poll."""
    company_id: int
    name: str
    slug: str
    country_iso: str = ""
    url: str = ""
    source_entry: str = ""                       # WatchEntry.key()
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    city: Optional[str] = None

    @property
    def uid(self) -> str:
        geo = f"@{self.city}" if self.city else ""
        return f"{self.slug or self.company_id}{geo}"


class Watchlist:
    def __init__(self, path: Path):
        self.path = Path(path)
        self._lock = threading.Lock()
        self.entries: list[WatchEntry] = []
        self.load()

    def load(self):
        if self.path.exists():
            data = yaml.safe_load(self.path.read_text()) or {}
            self.entries = [WatchEntry(**e) for e in data.get("watch", [])]
        else:
            self.entries = []
        log.info("watchlist loaded: %d entries", len(self.entries))

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(yaml.safe_dump(
            {"watch": [e.model_dump(exclude_none=True) for e in self.entries]}, sort_keys=False))

    def add(self, entry: WatchEntry) -> WatchEntry:
        with self._lock:
            self.entries = [e for e in self.entries if e.key() != entry.key()]
            self.entries.append(entry)
            self.save()
        return entry

    def remove(self, key: str) -> bool:
        with self._lock:
            before = len(self.entries)
            self.entries = [e for e in self.entries if e.key() != key]
            self.save()
            return len(self.entries) != before


def host_to_search_term(url: str) -> str:
    """'https://www.chase.com/personal' -> 'chase'"""
    host = urlparse(url if "://" in url else f"https://{url}").hostname or url
    host = re.sub(r"^(www|m|app|my)\.", "", host)
    parts = host.split(".")
    return parts[0] if len(parts) >= 2 else host
