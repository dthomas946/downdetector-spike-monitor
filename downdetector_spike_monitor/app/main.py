"""
REST API + service entrypoint.

    uvicorn app.main:app --host 0.0.0.0 --port 8099

Config comes from environment variables (or /data/options.json when running
as a Home Assistant add-on — see settings()).

Endpoints
  GET  /health                        liveness + last poll
  GET  /status                        everything: targets, states, active spikes
  GET  /spikes                        only active spikes
  GET  /companies/{uid}               one company's state
  GET  /search?name=&country=         look up Downdetector companies to add
  GET  /watchlist                     current watch entries
  POST /watchlist                     add {kind, value, country?, latitude?, longitude?, companies?}
  DELETE /watchlist/{key}             remove by key  (kind:value:country)
  POST /poll                          force an immediate poll
  GET  /config                        effective spike thresholds
"""
from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from .backends import EnterpriseBackend, UnofficialBackend
from .dd_client import DowndetectorClient
from .ha_mqtt import HAPublisher
from .monitor import Monitor
from .spike import SpikeConfig
from .watchlist import Watchlist, WatchEntry

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("dd.main")


def settings() -> dict:
    """Env vars win; otherwise HA add-on options.json; otherwise defaults."""
    opts = {}
    p = Path(os.environ.get("OPTIONS_FILE", "/data/options.json"))
    if p.exists():
        opts = json.loads(p.read_text())

    def g(key, default=None, cast=str):
        v = os.environ.get(key.upper(), opts.get(key.lower(), default))
        if v is None or v == "":
            return default
        if cast is bool:
            return str(v).lower() in ("1", "true", "yes", "on")
        return cast(v)

    return {
        "backend": g("backend", "unofficial"),
        "flaresolverr_url": g("flaresolverr_url", ""),
        "request_spacing": g("request_spacing", 3.0, float),
        "dd_client_id": g("dd_client_id", ""),
        "dd_client_secret": g("dd_client_secret", ""),
        "dd_base_url": g("dd_base_url", "https://downdetectorapi.com/v2"),
        "poll_interval": g("poll_interval", 300, int),
        "watchlist_file": g("watchlist_file", "/data/watchlist.yaml"),
        "mqtt_host": g("mqtt_host", ""),
        "mqtt_port": g("mqtt_port", 1883, int),
        "mqtt_user": g("mqtt_user", ""),
        "mqtt_password": g("mqtt_password", ""),
        "ha_webhook_url": g("ha_webhook_url", ""),
        "spike": SpikeConfig(
            ratio_multiplier=g("spike_ratio", 3.0, float),
            z_threshold=g("spike_z", 3.0, float),
            absolute_min_reports=g("spike_min_reports", 20, int),
            warn_on_warning=g("spike_warn_on_warning", False, bool),
            clear_after=g("spike_clear_after", 2, int),
        ),
    }


def build_monitor(cfg: dict) -> Monitor:
    if cfg["backend"] == "unofficial":
        backend = UnofficialBackend(flaresolverr_url=cfg["flaresolverr_url"],
                                    request_spacing=cfg["request_spacing"])
    else:
        if not cfg["dd_client_id"]:
            log.warning("DD_CLIENT_ID not set — enterprise backend will fail until configured")
        backend = EnterpriseBackend(DowndetectorClient(
            cfg["dd_client_id"], cfg["dd_client_secret"], base_url=cfg["dd_base_url"]))
    wl = Watchlist(Path(cfg["watchlist_file"]))
    pub = HAPublisher(cfg["mqtt_host"], cfg["mqtt_port"], cfg["mqtt_user"], cfg["mqtt_password"],
                      webhook_url=cfg["ha_webhook_url"])
    return Monitor(backend, wl, pub, cfg["spike"], poll_interval=cfg["poll_interval"])


monitor: Monitor | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global monitor
    monitor = build_monitor(settings())
    monitor.start()
    yield
    monitor.stop()


app = FastAPI(title="Downdetector Spike Monitor", version="0.1.0", lifespan=lifespan)


def _m() -> Monitor:
    if monitor is None:
        raise HTTPException(503, "monitor not started")
    return monitor


@app.get("/health")
def health():
    m = _m()
    return {"ok": m.last_error is None, "last_poll": m.last_poll, "last_error": m.last_error,
            "backend": m.backend.name, "targets": len(m.targets)}


@app.get("/status")
def status():
    return _m().snapshot()


@app.get("/spikes")
def spikes():
    return _m().snapshot()["active_spikes"]


@app.get("/companies/{uid}")
def company(uid: str):
    st = _m().states.get(uid)
    if not st:
        raise HTTPException(404, f"unknown target {uid}; see /status")
    return st.to_dict()


@app.get("/search")
def search(name: str = Query(..., min_length=2), country: str = ""):
    try:
        return _m().backend.search(name, country)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, str(e))


@app.get("/watchlist")
def watchlist():
    return [e.model_dump(exclude_none=True) | {"key": e.key()} for e in _m().watchlist.entries]


@app.post("/watchlist", status_code=201)
def add_watch(entry: WatchEntry):
    if entry.kind == "city" and (entry.latitude is None or entry.longitude is None or not entry.companies):
        raise HTTPException(422, "kind=city needs latitude, longitude and companies[]")
    m = _m()
    m.watchlist.add(entry)
    m.trigger()
    return entry.model_dump(exclude_none=True) | {"key": entry.key()}


@app.delete("/watchlist/{key:path}")
def del_watch(key: str):
    m = _m()
    if not m.watchlist.remove(key):
        raise HTTPException(404, "no such entry")
    m.trigger()
    return JSONResponse({"removed": key})


@app.post("/poll")
def poll():
    _m().trigger()
    return {"queued": True}


@app.get("/config")
def config():
    m = _m()
    return {"spike": m.cfg.model_dump(), "poll_interval": m.poll_interval, "backend": m.backend.name}
