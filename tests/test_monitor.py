import base64
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
