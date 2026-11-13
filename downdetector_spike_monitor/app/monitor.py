"""Polling loop: resolve watchlist -> poll backend -> evaluate spikes -> publish."""
from __future__ import annotations

import logging
import threading
import time
from typing import Optional

from .backends import Backend, Snapshot
from .ha_mqtt import HAPublisher
from .national import (NationalState, WidespreadConfig, check_cloudflare, evaluate_widespread,
                       next_scan_time)
from .spike import SpikeConfig, SpikeState, evaluate
from .watchlist import Watchlist, Target

log = logging.getLogger("dd.monitor")


class Monitor:
    def __init__(self, backend: Backend, watchlist: Watchlist, publisher: HAPublisher,
                 spike_cfg: SpikeConfig, poll_interval: int = 300, resolve_interval: int = 3600,
                 widespread_cfg: Optional[WidespreadConfig] = None, national_scan_minute: int = 0):
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
        # national scan (homepage) — hourly at HH:<national_scan_minute>
        self.wcfg = widespread_cfg or WidespreadConfig()
        self.national_scan_minute = national_scan_minute % 60
        self.national: dict[str, NationalState] = {}
        self.next_national_scan = next_scan_time(self.national_scan_minute)
        self._scan_now = threading.Event()

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

    def scan_now(self):
        """Force an immediate national scan (doesn't move the hourly schedule)."""
        self._scan_now.set()
        self._wake.set()

    def _run(self):
        self.publisher.online()
        next_poll = 0.0
        while not self._stop.is_set():
            now = time.time()
            if self._scan_now.is_set() or now >= self.next_national_scan:
                scheduled = not self._scan_now.is_set()
                self._scan_now.clear()
                try:
                    self.run_national_scans()
                except Exception as e:  # noqa: BLE001
                    log.exception("national scan failed: %s", e)
                if scheduled or now >= self.next_national_scan:
                    self.next_national_scan = next_scan_time(self.national_scan_minute)
            if time.time() >= next_poll or self._wake.is_set():
                self._wake.clear()
                try:
                    self.run_once()
                except Exception as e:  # noqa: BLE001
                    self.last_error = str(e)
                    log.exception("poll failed")
                next_poll = time.time() + self.poll_interval
            wait = max(1.0, min(next_poll, self.next_national_scan) - time.time())
            self._wake.wait(wait)

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
        got = {sn.target.uid for sn in snaps}
        snaps += self._from_national([t for t in targets if t.uid not in got])
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
                "source": snap.extra.get("source", "status_page"),
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
    # ------------------------------------------------------- national scan
    def countries(self) -> list[str]:
        return sorted({(e.value or "US").upper() for e in self.watchlist.entries
                       if e.enabled and e.kind == "country"})

    def run_national_scans(self):
        if not hasattr(self.backend, "national_scan"):
            return
        for iso in self.countries():
            self.run_national_scan(iso)

    def run_national_scan(self, iso: str) -> NationalState:
        st = self.national.get(iso) or NationalState(country=iso)
        try:
            rows = self.backend.national_scan(iso)
        except Exception as e:  # noqa: BLE001
            st.error = str(e)
            st.scanned_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            self.national[iso] = st
            log.warning("national scan %s failed: %s", iso, e)
            self.publisher.publish_national(st.summary())
            return st
        st.error = None
        st.companies = rows
        st.scans += 1
        st.scanned_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        affected = evaluate_widespread(self.wcfg, rows)
        was = st.widespread
        st.affected = affected
        st.widespread = len(affected) >= self.wcfg.min_services
        if st.widespread and self.wcfg.check_cloudflare:
            st.cloudflare = check_cloudflare()
        if st.widespread and not was:
            st.widespread_since, st.widespread_cleared_at = st.scanned_at, None
        elif was and not st.widespread:
            st.widespread_cleared_at = st.scanned_at
        self.national[iso] = st
        log.info("national scan %s: %d services listed, %d in danger, widespread=%s (%d affected)%s",
                 iso, len(rows), sum(r["status"] == "danger" for r in rows), st.widespread,
                 len(affected), f", cloudflare={st.cloudflare.get('indicator')}" if st.cloudflare and st.widespread else "")
        summ = st.summary()
        self.publisher.publish_national(summ)
        if st.widespread and not was:
            self.publisher.publish_raw_event({"event": "widespread_outage_started", **summ})
        elif was and not st.widespread:
            self.publisher.publish_raw_event({"event": "widespread_outage_cleared", **summ})
        return st

    def _from_national(self, targets: list[Target]) -> list[Snapshot]:
        """Watched companies whose own page couldn't be fetched: use the latest national
        scan if it lists them (i.e. they're among the most-reported services right now)."""
        out = []
        for t in targets:
            st = self.national.get((t.country_iso or "US").upper())
            if not st or not st.companies:
                continue
            row = next((c for c in st.companies if c["slug"] == t.slug), None)
            if row:
                out.append(Snapshot(t, row["sparkline"][-96:], row["baseline_est"], row["status"],
                                    {"category": row["category"], "source": "national_scan",
                                     "scanned_at": st.scanned_at}))
        return out

    def national_view(self) -> list[dict]:
        out = []
        for iso, st in sorted(self.national.items()):
            d = st.summary()
            d["companies"] = st.companies
            d["list_min_reports"] = self.wcfg.min_reports
            d["next_scan"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.next_national_scan))
            out.append(d)
        for iso in self.countries():
            if iso not in self.national:
                out.append({"country": iso, "scanned_at": None, "companies": [],
                            "next_scan": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.next_national_scan))})
        return out

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
