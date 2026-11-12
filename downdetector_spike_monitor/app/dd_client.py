"""
Downdetector Enterprise API v2 client.

Auth flow (client-credentials OAuth2):
    POST https://downdetectorapi.com/v2/tokens?grant_type=client_credentials
    HTTP Basic auth  -> username = Client ID, password = Client Secret
    -> {"access_token": "...", "expires_in": 3600, "token_type": "Bearer"}

All other calls:  Authorization: Bearer <access_token>

Endpoints used:
    GET /companies/search?name=&country=&fields=
    GET /companies?ids=1,2,3&fields=id,name,slug,status,baseline_current,stats_24,stats_60,country_iso
    GET /companies/{id}
    GET /companies/{id}/status?threshold=
    GET /reports?slugs=&interval=15m&startdate=&enddate=&latitude=&longitude=
    GET /sites            (one Downdetector site per country: downdetector.com, .co.uk, ...)
    GET /sites/{id}/companies
    GET /incidents?only_active=true
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

import httpx

log = logging.getLogger("dd.client")

DEFAULT_BASE_URL = "https://downdetectorapi.com/v2"

# Fields we always want back for spike detection.
COMPANY_FIELDS = [
    "id", "name", "slug", "url", "status", "country_iso", "site_id",
    "category_id", "baseline_current", "stats_24", "stats_60",
]


@dataclass
class Token:
    access_token: str
    token_type: str = "Bearer"
    expires_at: float = 0.0  # epoch seconds

    def valid(self, grace: int = 300) -> bool:
        return bool(self.access_token) and time.time() < (self.expires_at - grace)


@dataclass
class Company:
    id: int
    name: str
    slug: str
    url: str = ""
    status: str = ""                 # success | warning | danger
    country_iso: str = ""
    site_id: Optional[int] = None
    category_id: Optional[int] = None
    baseline_current: int = 0        # expected reports / 15 min right now
    stats_24: list[int] = field(default_factory=list)   # 96 x 15-min buckets, newest last
    stats_60: int = 0                # reports in the last 60 min
    raw: dict = field(default_factory=dict)

    @property
    def stats_15(self) -> int:
        """Reports in the most recent 15-minute bucket."""
        return int(self.stats_24[-1]) if self.stats_24 else 0

    @classmethod
    def from_api(cls, d: dict) -> "Company":
        return cls(
            id=int(d.get("id")),
            name=d.get("name", ""),
            slug=d.get("slug", ""),
            url=d.get("url", "") or "",
            status=(d.get("status") or "").lower(),
            country_iso=(d.get("country_iso") or "").upper(),
            site_id=d.get("site_id"),
            category_id=d.get("category_id"),
            baseline_current=int(d.get("baseline_current") or d.get("baseline") or 0),
            stats_24=[int(x) for x in (d.get("stats_24") or [])],
            stats_60=int(d.get("stats_60") or 0),
            raw=d,
        )


class DowndetectorError(RuntimeError):
    pass


class DowndetectorClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 20.0,
        transport: Optional[httpx.BaseTransport] = None,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = base_url.rstrip("/")
        self._token = Token(access_token="")
        self._http = httpx.Client(timeout=timeout, transport=transport)

    # ------------------------------------------------------------------ auth
    def _fetch_token(self) -> Token:
        url = f"{self.base_url}/tokens"
        r = self._http.post(
            url,
            params={"grant_type": "client_credentials"},
            auth=(self.client_id, self.client_secret),
        )
        if r.status_code != 200:
            raise DowndetectorError(f"token request failed: {r.status_code} {r.text[:200]}")
        body = r.json()
        tok = Token(
            access_token=body["access_token"],
            token_type=body.get("token_type", "Bearer"),
            expires_at=time.time() + int(body.get("expires_in", 3600)),
        )
        log.info("obtained new Downdetector token (expires in %ss)", int(body.get("expires_in", 3600)))
        return tok

    def _headers(self) -> dict:
        if not self._token.valid():
            self._token = self._fetch_token()
        return {"Authorization": f"{self._token.token_type} {self._token.access_token}"}

    def _get(self, path: str, params: Optional[dict] = None) -> Any:
        url = f"{self.base_url}{path}"
        params = {k: v for k, v in (params or {}).items() if v is not None and v != ""}
        for attempt in (1, 2):
            r = self._http.get(url, params=params, headers=self._headers())
            if r.status_code == 401 and attempt == 1:
                self._token = Token(access_token="")  # force refresh, retry once
                continue
            if r.status_code == 429:
                raise DowndetectorError("rate limited (429) — increase poll_interval")
            if r.status_code >= 400:
                raise DowndetectorError(f"GET {path} -> {r.status_code}: {r.text[:200]}")
            return r.json()
        raise DowndetectorError("unreachable")

    @staticmethod
    def _unwrap(body: Any) -> list[dict]:
        """API responses are either a bare list or {"data": [...]} / {"companies": [...]}."""
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            for k in ("data", "companies", "results", "items"):
                if isinstance(body.get(k), list):
                    return body[k]
        return []

    # ------------------------------------------------------------- companies
    def search_companies(
        self, name: str = "", country: str = "", slug: str = "", category_id: Optional[int] = None,
        page_size: int = 25,
    ) -> list[Company]:
        body = self._get(
            "/companies/search",
            {"name": name, "country": country, "slug": slug, "category_id": category_id,
             "fields": ",".join(COMPANY_FIELDS), "page_size": page_size},
        )
        return [Company.from_api(d) for d in self._unwrap(body)]

    def get_companies(self, ids: Iterable[int]) -> list[Company]:
        """Bulk fetch — one call for all watched companies. Chunks at 50 ids."""
        ids = [int(i) for i in ids]
        out: list[Company] = []
        for i in range(0, len(ids), 50):
            chunk = ids[i:i + 50]
            body = self._get(
                "/companies",
                {"ids": ",".join(map(str, chunk)), "fields": ",".join(COMPANY_FIELDS)},
            )
            out.extend(Company.from_api(d) for d in self._unwrap(body))
        return out

    def get_company(self, company_id: int) -> Company:
        body = self._get(f"/companies/{company_id}", {"fields": ",".join(COMPANY_FIELDS)})
        if isinstance(body, dict) and "data" in body and isinstance(body["data"], dict):
            body = body["data"]
        return Company.from_api(body)

    def get_status(self, company_id: int, threshold: Optional[int] = None) -> str:
        body = self._get(f"/companies/{company_id}/status", {"threshold": threshold})
        if isinstance(body, dict):
            return str(body.get("status") or body.get("data", {}).get("status", "")).lower()
        return str(body).lower()

    # --------------------------------------------------------------- reports
    def get_reports(
        self, slugs: Iterable[str], interval: str = "15m",
        startdate: Optional[str] = None, enddate: Optional[str] = None,
        latitude: Optional[float] = None, longitude: Optional[float] = None,
    ) -> list[dict]:
        """Report counts over time; latitude/longitude narrows to a city area."""
        body = self._get(
            "/reports",
            {"slugs": ",".join(slugs), "interval": interval, "startdate": startdate,
             "enddate": enddate, "latitude": latitude, "longitude": longitude},
        )
        return self._unwrap(body) if not isinstance(body, dict) or "data" in body else [body]

    # ----------------------------------------------------------------- sites
    def list_sites(self) -> list[dict]:
        return self._unwrap(self._get("/sites"))

    def site_companies(self, site_id: int, page_size: int = 100) -> list[Company]:
        body = self._get(f"/sites/{site_id}/companies",
                         {"fields": ",".join(COMPANY_FIELDS), "page_size": page_size})
        return [Company.from_api(d) for d in self._unwrap(body)]

    def list_incidents(self, only_active: bool = True) -> list[dict]:
        return self._unwrap(self._get("/incidents", {"only_active": str(only_active).lower()}))

    def close(self):
        self._http.close()
