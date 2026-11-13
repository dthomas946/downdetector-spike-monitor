import base64
import time
import json
from pathlib import Path

import httpx
import pytest

from app.backends import EnterpriseBackend
from app.dd_client import DowndetectorClient
from app.ha_mqtt import HAPublisher
from app.monitor import Monitor
from app.spike import SpikeConfig, SpikeState, evaluate
from app.watchlist import Watchlist, WatchEntry, host_to_search_term


# ---------------------------------------------------------------- fake API
class FakeDD:
    """Minimal stand-in for downdetectorapi.com/v2."""

    def __init__(self):
        self.stats = {101: [3] * 95 + [4], 202: [10] * 95 + [12]}
        self.status = {101: "success", 202: "success"}
        self.token_calls = 0

    def handler(self, req: httpx.Request) -> httpx.Response:
        p = req.url.path
        if p.endswith("/tokens"):
            self.token_calls += 1
            auth = req.headers.get("authorization", "")
            assert auth.startswith("Basic ")
            assert base64.b64decode(auth[6:]).decode() == "cid:csecret"
            assert req.url.params["grant_type"] == "client_credentials"
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600, "token_type": "Bearer"})
        if req.headers.get("authorization") != "Bearer tok":
            return httpx.Response(401, json={"error": "unauthorized"})
        if p.endswith("/companies/search"):
            name, slug = req.url.params.get("name", ""), req.url.params.get("slug", "")
            rows = [self._co(101)] if slug == "aws" or name == "aws" else []
            if name == "chase":
                rows = [self._co(202)]
            return httpx.Response(200, json={"data": rows})
        if p.endswith("/companies"):
            ids = [int(i) for i in req.url.params["ids"].split(",")]
            return httpx.Response(200, json={"data": [self._co(i) for i in ids]})
        return httpx.Response(404)

    def _co(self, i):
        names = {101: ("Amazon Web Services", "aws", "https://aws.amazon.com"),
                 202: ("Chase", "chase", "https://www.chase.com")}
        n, s, u = names[i]
        return {"id": i, "name": n, "slug": s, "url": u, "status": self.status[i],
                "country_iso": "US", "baseline_current": 3 if i == 101 else 10,
                "stats_24": self.stats[i], "stats_60": sum(self.stats[i][-4:])}


class NullPub(HAPublisher):
    def __init__(self):
        super().__init__()
        self.events = []

    def publish_state(self, t, s):
        pass

    def publish_event(self, t, s, kind):
        self.events.append((t.uid, kind, s.current))


@pytest.fixture
def world(tmp_path: Path):
    fake = FakeDD()
    client = DowndetectorClient("cid", "csecret", base_url="https://x.test/v2",
                                transport=httpx.MockTransport(fake.handler))
    wl = Watchlist(tmp_path / "wl.yaml")
    wl.add(WatchEntry(kind="company", value="aws", country="US"))
    wl.add(WatchEntry(kind="url", value="https://www.chase.com/personal"))
    pub = NullPub()
    mon = Monitor(EnterpriseBackend(client), wl, pub, SpikeConfig(absolute_min_reports=20))
    return fake, mon, pub


# ----------------------------------------------------------------- tests
def test_host_term():
    assert host_to_search_term("https://www.chase.com/personal") == "chase"
    assert host_to_search_term("aws.amazon.com") == "aws"


def test_resolve_and_poll(world):
    fake, mon, pub = world
    mon.run_once()
    assert set(mon.targets) == {"aws", "chase"}
    assert fake.token_calls == 1
    assert not mon.snapshot()["active_spikes"]


def test_spike_lifecycle(world):
    fake, mon, pub = world
    mon.run_once()
    # AWS jumps from ~3 to 60 reports / 15 min
    fake.stats[101] = fake.stats[101][1:] + [60]
    fake.status[101] = "danger"
    mon.run_once()
    spikes = mon.snapshot()["active_spikes"]
    assert len(spikes) == 1 and spikes[0]["slug"] == "aws"
    assert spikes[0]["severity"] == "danger"
    assert any("downdetector status=danger" in r for r in spikes[0]["reasons"])
    assert pub.events[-1] == ("aws", "spike_started", 60)

    # one quiet bucket must NOT clear it (hysteresis)
    fake.stats[101] = fake.stats[101][1:] + [5]
    fake.status[101] = "success"
    mon.run_once()
    assert mon.states["aws"].active
    # second quiet poll clears
    fake.stats[101] = fake.stats[101][1:] + [4]
    mon.run_once()
    assert not mon.states["aws"].active
    assert pub.events[-1][1] == "spike_cleared"


def test_small_numbers_dont_alert():
    st = SpikeState(1, "x", "x")
    # 1 -> 6 reports is 6x baseline but below absolute_min_reports
    evaluate(SpikeConfig(), st, [1] * 20 + [6], baseline_current=1, dd_status="warning")
    assert not st.active


def test_zscore_rule_without_dd_status():
    st = SpikeState(1, "x", "x")
    evaluate(SpikeConfig(ratio_multiplier=100), st, [10, 11, 9, 10, 12, 10, 11, 10, 9, 10, 11, 10, 45],
             baseline_current=10, dd_status="")
    assert st.active and st.zscore > 3


def test_401_refreshes_token(tmp_path):
    fake = FakeDD()
    calls = {"n": 0}
    def flaky(req):
        if req.url.path.endswith("/companies") and calls["n"] == 0:
            calls["n"] += 1
            return httpx.Response(401)
        return fake.handler(req)
    c = DowndetectorClient("cid", "csecret", base_url="https://x.test/v2", transport=httpx.MockTransport(flaky))
    assert c.get_companies([101])[0].slug == "aws"
    assert fake.token_calls == 2


def test_watchlist_roundtrip(tmp_path):
    wl = Watchlist(tmp_path / "w.yaml")
    wl.add(WatchEntry(kind="city", value="Little Rock", latitude=34.7, longitude=-92.3, companies=["att"]))
    wl2 = Watchlist(tmp_path / "w.yaml")
    assert wl2.entries[0].companies == ["att"]
    assert wl2.remove("city:Little Rock:") and not wl2.entries


# ------------------------------------------------- unofficial (scraper) backend
FIX = Path(__file__).parent / "fixtures"


def _fake_fetch(url: str) -> str:
    if url.endswith("/companies/"):
        return (FIX / "companies_directory.html").read_text()
    if "/status/aws-amazon-web-services/" in url:
        return (FIX / "aws_status_2026-11-12.html").read_text()
    raise RuntimeError(f"{url} -> 404")


def test_unofficial_parse_real_page():
    from app.backends import UnofficialBackend
    d = UnofficialBackend.parse_page((FIX / "aws_status_2026-11-12.html").read_text())
    assert len(d["reports"]) == 96 and len(d["baseline"]) == 96
    assert d["reports"][-1] == 866 and d["baseline"][-1] == 13
    assert d["status"] == "danger"
    assert d["company_id"] == 10147 and d["name"] == "Amazon Web Services"
    assert d["category"] == "Foundational Internet"
    assert d["indicators"][0] == {"name": "EC2", "count": 91}
    assert "Chicago" in d["cities"]


def test_unofficial_end_to_end(tmp_path):
    from app.backends import UnofficialBackend
    be = UnofficialBackend(fetch=_fake_fetch)
    wl = Watchlist(tmp_path / "wl.yaml")
    wl.add(WatchEntry(kind="url", value="https://aws.amazon.com/console"))     # host -> "aws"
    wl.add(WatchEntry(kind="company", value="amazon web"))                      # fuzzy name
    pub = NullPub()
    mon = Monitor(be, wl, pub, SpikeConfig())
    mon.run_once()
    assert list(mon.targets) == ["aws-amazon-web-services"]                     # de-duplicated
    s = mon.states["aws-amazon-web-services"]
    assert s.active and s.severity == "danger" and s.current == 866 and s.company_id == 10147
    assert pub.events == [("aws-amazon-web-services", "spike_started", 866)]
    assert [h["slug"] for h in be.search("at")] [0] == "att"


def test_unofficial_429_retry_and_unescape(tmp_path):
    from app.backends import UnofficialBackend, RateLimited
    calls = {"aws": 0}
    def fetch(url):
        if url.endswith("/companies/"):
            return '<a title="AT&amp;T" href="/status/att/"></a>' + _fake_fetch(url)
        if "aws-amazon-web-services" in url:
            calls["aws"] += 1
            if calls["aws"] == 1:
                raise RateLimited("429")
        return _fake_fetch(url)
    be = UnofficialBackend(fetch=fetch, request_spacing=0, backoff=0)
    assert be.directory("US")["att"] == "AT&T"
    wl = Watchlist(tmp_path / "wl.yaml")
    wl.add(WatchEntry(kind="company", value="aws-amazon-web-services"))
    mon = Monitor(be, wl, NullPub(), SpikeConfig())
    mon.run_once()
    assert calls["aws"] == 2 and mon.states["aws-amazon-web-services"].active


def test_unofficial_circuit_breaker(tmp_path):
    from app.backends import UnofficialBackend, RateLimited
    hits = {"n": 0}
    def fetch(url):
        if url.endswith("/companies/"):
            return _fake_fetch(url)
        hits["n"] += 1
        raise RateLimited("429")
    be = UnofficialBackend(fetch=fetch, request_spacing=0, backoff=0)
    wl = Watchlist(tmp_path / "wl.yaml")
    for slug in ("aws-amazon-web-services", "att", "entergy"):
        wl.add(WatchEntry(kind="company", value=slug))
    mon = Monitor(be, wl, NullPub(), SpikeConfig())
    mon.run_once()
    assert hits["n"] == 2                    # first target + its retry, then stop
    assert be.cooldown_until > 0 and be._cooldown == 300
    mon.run_once()
    assert hits["n"] == 2                    # still cooling down: no requests sent


# ------------------------------------------------------------- national scan
def _home_fetch(url):
    if url.rstrip("/").endswith("downdetector.com"):
        return (FIX / "homepage_2026-11-12.html").read_text()
    return _fake_fetch(url)


def test_parse_homepage_real_format():
    from app.national import parse_homepage
    rows = parse_homepage((FIX / "homepage_2026-11-12.html").read_text())
    assert [r["slug"] for r in rows][:2] == ["outlook", "microsoft-365"] and len(rows) == 7
    o = rows[0]
    assert o["status"] == "danger" and o["current"] == 30268 and o["peak_24h"] == 129476
    assert o["baseline_est"] < 20 and o["category"] == "email" and o["id"] == 10205
    assert rows[5]["name"] == "AT&T"


def test_widespread_and_cloudflare(tmp_path, monkeypatch):
    from app import national
    from app.backends import UnofficialBackend
    from app.national import WidespreadConfig
    calls = []
    def fake_cf(*a, **k):
        calls.append(1)
        return {"indicator": "none", "incidents": [], "likely_cause": False}
    import app.monitor as monmod
    monkeypatch.setattr(monmod, "check_cloudflare", fake_cf)
    be = UnofficialBackend(fetch=_home_fetch, request_spacing=0, backoff=0)
    wl = Watchlist(tmp_path / "wl.yaml")
    wl.add(WatchEntry(kind="country", value="US"))
    pub = NullPub(); raw = []
    pub.publish_national = lambda s: None
    pub.publish_raw_event = lambda p: raw.append(p)
    mon = Monitor(be, wl, pub, SpikeConfig(), widespread_cfg=WidespreadConfig(min_services=3, min_reports=1000))
    assert mon.countries() == ["US"]
    mon.run_once()
    assert mon.targets == {}                        # country entry -> no per-company targets
    st = mon.run_national_scan("US")
    # Outlook, M365, Store (max last hr 1938), Copilot (1163) >= 1000 & danger; Gmail 516 < 1000
    assert st.widespread and [a["slug"] for a in st.affected] == ["outlook", "microsoft-365", "microsoft", "microsoft-copilot"]
    assert raw[-1]["event"] == "widespread_outage_started" and calls == [1]
    assert raw[-1]["cloudflare"]["indicator"] == "none"
    mon.run_national_scan("US")                       # still widespread: no duplicate event
    assert len(raw) == 1


def test_watched_company_falls_back_to_national(tmp_path):
    from app.backends import UnofficialBackend, RateLimited
    def fetch(url):
        if "/status/" in url:
            raise RateLimited("429")
        return _home_fetch(url) if url.rstrip("/").endswith(".com") else _fake_fetch(url)
    dirhtml = '<a title="Microsoft Outlook" href="/status/outlook/"></a>'
    be = UnofficialBackend(fetch=fetch, request_spacing=0, backoff=0,
                           site_fetch=lambda u: dirhtml if u.endswith("/companies/") else _home_fetch(u))
    wl = Watchlist(tmp_path / "wl.yaml")
    wl.add(WatchEntry(kind="country", value="US"))
    wl.add(WatchEntry(kind="company", value="outlook", country="US"))
    pub = NullPub()
    pub.publish_national = lambda s: None
    pub.publish_raw_event = lambda p: None
    mon = Monitor(be, wl, pub, SpikeConfig())
    mon.run_national_scan("US")
    mon.run_once()
    s = mon.states["outlook"]
    assert s.active and s.current == 30268 and mon.details["outlook"]["source"] == "national_scan"


def test_next_scan_time_top_of_hour():
    from app.national import next_scan_time
    import calendar
    t = calendar.timegm((2026, 11, 12, 23, 52, 10, 0, 0, 0))
    assert time.gmtime(next_scan_time(0, t))[3:6] == (0, 0, 0)       # 00:00:00 next day
    assert time.gmtime(next_scan_time(55, t))[3:6] == (23, 55, 0)


def test_cloudflare_summary_parse():
    from app.national import check_cloudflare
    body = {"status": {"indicator": "major", "description": "Partial System Outage"},
            "incidents": [{"name": "Elevated errors", "status": "investigating", "impact": "major",
                           "shortlink": "https://stspg.io/x"}],
            "components": [{"name": "CDN/Cache", "status": "major_outage"}, {"name": "DNS", "status": "operational"}]}
    r = check_cloudflare(get=lambda u: body)
    assert r["likely_cause"] and r["degraded_components"] == ["CDN/Cache"] and r["incidents"][0]["impact"] == "major"


def test_flaresolverr_recovers_from_crashed_tab(monkeypatch):
    from app.backends import FlareSolverr
    monkeypatch.setattr("app.backends.time.sleep", lambda s: None)
    calls = []
    def fake_call(self, payload):
        calls.append(payload["cmd"])
        if payload["cmd"] == "request.get" and calls.count("request.get") == 1:
            return {"status": "error", "message": "Error solving the challenge. Message: tab crashed"}
        if payload["cmd"] == "request.get":
            return {"status": "ok", "solution": {"status": 200, "response": "<html>ok</html>"}}
        return {"status": "ok"}
    monkeypatch.setattr(FlareSolverr, "_call", fake_call)
    fs = FlareSolverr("http://x/v1")
    assert fs.get("https://downdetector.com/") == "<html>ok</html>"
    assert calls == ["request.get", "request.get"]            # no persistent session


def test_restart_does_not_reannounce(tmp_path, monkeypatch):
    """State survives a restart: an ongoing spike / widespread outage is not re-announced."""
    import app.monitor as monmod
    from app.backends import UnofficialBackend
    monkeypatch.setattr(monmod, "check_cloudflare", lambda *a, **k: {"indicator": "none"})
    sf = tmp_path / "state.json"
    def build():
        be = UnofficialBackend(fetch=_home_fetch, request_spacing=0, backoff=0)
        wl = Watchlist(tmp_path / "wl.yaml")
        pub = NullPub(); raw = []
        pub.publish_national = lambda s: None
        pub.publish_raw_event = lambda p: raw.append(p["event"])
        return Monitor(be, wl, pub, SpikeConfig(), state_file=sf), pub, raw, wl
    m1, pub1, raw1, wl = build()
    wl.add(WatchEntry(kind="country", value="US"))
    wl.add(WatchEntry(kind="company", value="aws-amazon-web-services"))
    m1.run_national_scan("US"); m1.run_once()
    assert raw1 == ["widespread_outage_started"] and pub1.events[-1][1] == "spike_started"
    m2, pub2, raw2, _ = build()                       # "restart"
    assert m2.states["aws-amazon-web-services"].active and m2.national["US"].widespread
    m2.run_national_scan("US"); m2.run_once()
    assert raw2 == [] and pub2.events == []           # nothing re-announced


def test_watched_spike_clears_when_it_drops_off_national_list(tmp_path):
    from app.backends import UnofficialBackend, RateLimited
    home = (FIX / "homepage_2026-11-12.html").read_text()
    pages = {"home": home}
    def site(u):
        return '<a title="Microsoft Outlook" href="/status/outlook/"></a>' if u.endswith("/companies/") else pages["home"]
    def fetch(u):
        raise RateLimited("429")
    be = UnofficialBackend(fetch=fetch, site_fetch=site, request_spacing=0, backoff=0)
    be.cooldown_min = 0
    wl = Watchlist(tmp_path / "wl.yaml")
    wl.add(WatchEntry(kind="country", value="US"))
    wl.add(WatchEntry(kind="company", value="outlook", country="US"))
    pub = NullPub(); nat = []
    pub.publish_national = lambda s: nat.append(s)
    pub.publish_raw_event = lambda p: None
    mon = Monitor(be, wl, pub, SpikeConfig(clear_after=2))
    mon.run_once(); mon.run_national_scan("US"); mon.run_once()
    assert mon.states["outlook"].active and pub.events[-1] == ("outlook", "spike_started", 30268)
    assert nat[-1]["watched_in_list"][0]["slug"] == "outlook"
    # Outlook recovers and drops out of the top list
    pages["home"] = home.split('{\\"__typename\\":\\"CompanyType\\",\\"id\\":\\"10205\\"')[0] + \
        '{\\"__typename\\":\\"CompanyType\\",\\"id\\":\\"34532' + home.split('{\\"__typename\\":\\"CompanyType\\",\\"id\\":\\"34532')[1]
    mon.run_national_scan("US")
    mon.run_once(); mon.run_once()
    assert not mon.states["outlook"].active and pub.events[-1][1] == "spike_cleared"


# ------------------------------------------------------------------ severity
def test_severity_needs_ratio_and_volume():
    from app.spike import severity
    small = severity(45, 15)            # 3x, small service
    big = severity(3000, 1000)          # 3x, big service
    busy = severity(30000, 20000)       # 1.5x on a huge base
    total = severity(300, 10)           # 30x small service
    outlook = severity(129476, 5)
    assert small == (2.4, "elevated") and big[1] == "minor" and big[0] > small[0]
    assert busy[1] == "elevated" and total[1] == "major" and outlook[1] == "critical"
    assert severity(10, 15) == (0.0, "normal")


def test_baseline_p20_resists_long_outage():
    from app.national import estimate_baseline
    # outage covers 70% of the older window: median would be huge, P20 stays low
    spark = [5] * 25 + [20000] * 59 + [15000] * 12
    assert estimate_baseline(spark) == 5


def test_learned_baseline_keeps_7day_minimum(tmp_path):
    from app.backends import UnofficialBackend
    be = UnofficialBackend(fetch=_home_fetch, request_spacing=0, backoff=0)
    pub = NullPub(); pub.publish_national = lambda s: None; pub.publish_raw_event = lambda p: None
    mon = Monitor(be, Watchlist(tmp_path / "wl.yaml"), pub, SpikeConfig())
    # Gmail's window estimate is 3 (its sparkline is quiet before the spike); pretend we saw
    # a quieter day earlier this week, and a polluted (outage-filled) one for Outlook.
    mon.baselines["US:gmail"] = [[int(time.time()) - 86400, 1]]
    mon.cfg.min_baseline = 1                                              # isolate the 7-day-min logic
    mon.baselines["US:outlook"] = [[int(time.time()) - 3600, 9000]]
    st = mon.run_national_scan("US")
    g = next(c for c in st.companies if c["slug"] == "gmail")
    o = next(c for c in st.companies if c["slug"] == "outlook")
    assert g["baseline_window"] == 3 and g["baseline_est"] == 1          # 7-day minimum wins
    assert o["baseline_est"] == max(1, o["baseline_window"]) < 9000       # polluted history ignored
    assert o["severity_level"] == "critical"
    assert all("severity_score" in c for c in st.companies)
    # sorted by severity: outlook first among the >1000 rows in the UI; check score ordering sanity
    m365 = next(c for c in st.companies if c["slug"] == "microsoft-365")
    assert o["severity_score"] > m365["severity_score"]


def test_national_baseline_floor(tmp_path):
    from app.backends import UnofficialBackend
    be = UnofficialBackend(fetch=_home_fetch, request_spacing=0, backoff=0)
    pub = NullPub(); pub.publish_national = lambda s: None; pub.publish_raw_event = lambda p: None
    mon = Monitor(be, Watchlist(tmp_path / "wl.yaml"), pub, SpikeConfig(min_baseline=5))
    st = mon.run_national_scan("US")
    assert min(c["baseline_est"] for c in st.companies) >= 5


def test_dd_status_floors_severity():
    from app.spike import severity
    assert severity(1146, 4369, "danger") == (3.0, "minor")      # polluted baseline, DD says danger
    assert severity(10, 15, "warning") == (1.0, "elevated")
    assert severity(2558, 5, "danger")[1] == "critical"            # floor never lowers a score
