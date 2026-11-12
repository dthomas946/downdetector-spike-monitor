"""Polling loop: resolve watchlist -> poll backend -> evaluate spikes -> publish."""
from __future__ import annotations

import logging
import threading
import time
from typing import Optional

from .backends import Backend, Snapshot
from .ha_mqtt import HAPublisher
from .spike import SpikeConfig, SpikeState, evaluate
from .watchlist import Watchlist, Target

log = logging.getLogger("dd.monitor")


class Monitor:
    def __init__(self, backend: Backend, watchlist: Watchlist, publisher: HAPublisher,
                 spike_cfg: SpikeConfig, poll_interval: int = 300, resolve_interval: int = 3600):
        self.backend = backend
        self.watchlist = watchlist
        self.publisher = publisher
        self.cfg = spike_cfg
        self.poll_interval = max(60, poll_interval)
        self.resolve_interval = resolve_interval
        self.targets: dict[str, Target] = {}
        self.states: dict[str, SpikeState] = {}
        self.details: dict[str, dict] = {}       # uid -> series + extras for the UI
        self.last_poll: Optional[str] = None
        self.last_error: Optional[str] = None
        self._last_resolve = 0.0
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------ lifecycle
    def start(self):
        self._thread = threading.Thread(target=self._run, name="dd-monitor", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._wake.set()

    def trigger(self):
        """Force an immediate resolve+poll (e.g. after watchlist edit)."""
        self._last_resolve = 0
        self._wake.set()

    def _run(self):
        self.publisher.online()
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception as e:  # noqa: BLE001
                self.last_error = str(e)
                log.exception("poll failed")
            self._wake.wait(self.poll_interval)
            self._wake.clear()

    # ------------------------------------------------------------- one pass
    def resolve_targets(self):
        new: dict[str, Target] = {}
        for entry in self.watchlist.entries:
            if not entry.enabled:
                continue
            try:
                for t in self.backend.resolve(entry):
                    new[t.uid] = t
            except Exception as e:  # noqa: BLE001
                log.warning("resolve failed for %s: %s", entry.key(), e)
        with self._lock:
            self.targets = new
            for uid in list(self.states):
                if uid not in new:
                    del self.states[uid]
                    self.details.pop(uid, None)
        self._last_resolve = time.time()
        log.info("resolved %d targets from %d watchlist entries", len(new), len(self.watchlist.entries))

    def run_once(self):
        if time.time() - self._last_resolve > self.resolve_interval or not self.targets:
            self.resolve_targets()
        targets = list(self.targets.values())
        if not targets:
            return
        snaps = self.backend.poll(targets)
        for snap in snaps:
            self._apply(snap)
        self.last_poll = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self.last_error = None

    def _apply(self, snap: Snapshot):
        t = snap.target
        with self._lock:
            st = self.states.get(t.uid) or SpikeState(company_id=t.company_id, name=t.name, slug=t.slug)
            was_active, was_sev = st.active, st.severity
            evaluate(self.cfg, st, snap.stats_24, snap.baseline_current, snap.dd_status)
            self.states[t.uid] = st
            self.details[t.uid] = {
                "uid": t.uid, "name": t.name, "slug": t.slug, "url": t.url,
                "country_iso": t.country_iso, "city": t.city, "source_entry": t.source_entry,
                "reports": list(snap.stats_24[-96:]),
                "baseline": snap.extra.get("baseline_series") or [snap.baseline_current] * len(snap.stats_24[-96:]),
                "timestamps": snap.extra.get("timestamps", []),
                "indicators": snap.extra.get("indicators", [])[:5],
                "cities": snap.extra.get("cities", [])[:10],
                "category": snap.extra.get("category", ""),
                "state": st.to_dict(),
            }
        self.publisher.publish_state(t, st)
        if st.active and not was_active:
            self.publisher.publish_event(t, st, "spike_started")
        elif st.active and was_sev == "warning" and st.severity == "danger":
            self.publisher.publish_event(t, st, "spike_escalated")
        elif was_active and not st.active:
            self.publisher.publish_event(t, st, "spike_cleared")

    # --------------------------------------------------------------- views
    def details_view(self) -> list[dict]:
        with self._lock:
            rows = [dict(d, state=self.states[d["uid"]].to_dict()) for d in self.details.values()
                    if d["uid"] in self.states]
        rows.sort(key=lambda r: (not r["state"]["active"], -r["state"]["ratio"], r["name"].lower()))
        return rows

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "last_poll": self.last_poll,
                "last_error": self.last_error,
                "backend": self.backend.name,
                "targets": len(self.targets),
                "active_spikes": [s.to_dict() for s in self.states.values() if s.active],
                "companies": {uid: s.to_dict() for uid, s in self.states.items()},
            }
