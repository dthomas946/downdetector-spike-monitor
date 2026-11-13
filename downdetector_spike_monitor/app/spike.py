"""
Spike detection on Downdetector report counts.

Downdetector gives us, per company:
  - stats_24          : 96 buckets of 15-minute report counts (newest last)
  - baseline_current  : the expected count for the current 15-min window
  - status            : Downdetector's own verdict  success | warning | danger

A "spike" is declared when ANY of these fire (all are tunable):

  1. status rule   : Downdetector says 'danger' (or 'warning' if warn_on_warning)
  2. ratio rule    : current >= ratio_multiplier * max(baseline_current, min_baseline)
  3. z-score rule  : (current - mean(recent)) / stdev(recent) >= z_threshold,
                     using the previous N buckets as the reference window
  4. absolute rule : current >= absolute_min_reports  (guards against tiny baselines)

Rules 2 and 3 only fire when current >= absolute_min_reports, so a jump from
1 to 4 reports on a sleepy service doesn't page you.

Hysteresis: a spike clears only when current drops below clear_fraction * trigger
level for `clear_after` consecutive polls, so a single quiet bucket mid-outage
doesn't flap the alert.
"""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel


# Severity: needs BOTH a large multiple of normal AND a meaningful number of extra
# reports. 3x on a baseline of 15 (45 reports) scores ~2.4; 3x on 1,000 scores ~5.2;
# a 26,000x outage with 129k reports scores ~75.
SEVERITY_LEVELS = [(20, "critical"), (8, "major"), (3, "minor"), (1, "elevated"), (0, "normal")]


def severity(current: float, baseline: float) -> tuple[float, str]:
    baseline = max(1.0, float(baseline or 0))
    current = float(current or 0)
    if current <= baseline:
        return 0.0, "normal"
    score = math.log2(current / baseline) * math.log10(current - baseline + 1)
    level = next(name for floor, name in SEVERITY_LEVELS if score >= floor)
    return round(score, 1), level


class SpikeConfig(BaseModel):
    ratio_multiplier: float = 3.0        # current >= 3x baseline
    z_threshold: float = 3.0             # or 3 std devs above recent mean
    z_window: int = 12                   # previous 12 buckets = 3 hours
    absolute_min_reports: int = 20       # never alert below this many reports/15m
    min_baseline: int = 5                # floor for baseline in ratio math
    warn_on_warning: bool = False        # treat DD 'warning' as a spike too
    clear_fraction: float = 0.5          # clear when below 50% of the level that triggered
    clear_after: int = 2                 # ... for this many consecutive polls


@dataclass
class SpikeState:
    """Per-company state carried between polls."""
    company_id: int
    name: str
    slug: str
    active: bool = False
    severity: str = "none"               # none | warning | danger
    triggered_at: Optional[str] = None
    cleared_at: Optional[str] = None
    trigger_level: int = 0               # report count when the spike started
    peak: int = 0
    current: int = 0
    baseline: int = 0
    ratio: float = 0.0
    zscore: float = 0.0
    dd_status: str = ""
    reasons: list[str] = field(default_factory=list)
    severity_score: float = 0.0
    severity_level: str = "normal"       # normal | elevated | minor | major | critical
    _quiet_polls: int = 0
    last_updated: Optional[str] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("_quiet_polls", None)
        return d


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def evaluate(cfg: SpikeConfig, state: SpikeState, stats_24: list[int],
             baseline_current: int, dd_status: str) -> SpikeState:
    """Update `state` in place from a fresh poll and return it."""
    current = int(stats_24[-1]) if stats_24 else 0
    history = [int(x) for x in stats_24[:-1]][-cfg.z_window:] if len(stats_24) > 1 else []

    baseline = max(int(baseline_current), cfg.min_baseline)
    ratio = current / baseline if baseline else 0.0

    zscore = 0.0
    if len(history) >= 4:
        mean = statistics.fmean(history)
        stdev = statistics.pstdev(history) or 1.0
        zscore = (current - mean) / stdev

    reasons: list[str] = []
    dd_status = (dd_status or "").lower()
    if dd_status == "danger":
        reasons.append("downdetector status=danger")
    elif dd_status == "warning" and cfg.warn_on_warning:
        reasons.append("downdetector status=warning")

    if current >= cfg.absolute_min_reports:
        if ratio >= cfg.ratio_multiplier:
            reasons.append(f"{current} reports is {ratio:.1f}x baseline {baseline}")
        if zscore >= cfg.z_threshold:
            reasons.append(f"z-score {zscore:.1f} vs last {len(history)} buckets")

    spiking_now = bool(reasons)

    state.current = current
    state.baseline = baseline
    state.ratio = round(ratio, 2)
    state.zscore = round(zscore, 2)
    state.dd_status = dd_status
    state.severity_score, state.severity_level = severity(current, baseline)
    state.last_updated = _now()

    if spiking_now:
        if not state.active:
            state.active = True
            state.triggered_at = _now()
            state.cleared_at = None
            state.trigger_level = current
            state.peak = current
        state.peak = max(state.peak, current)
        state._quiet_polls = 0
        state.reasons = reasons
        state.severity = "danger" if (dd_status == "danger" or ratio >= cfg.ratio_multiplier * 2) else "warning"
    elif state.active:
        # hysteresis: require sustained drop before clearing
        if current < cfg.clear_fraction * max(state.trigger_level, cfg.absolute_min_reports):
            state._quiet_polls += 1
        else:
            state._quiet_polls = 0
        if state._quiet_polls >= cfg.clear_after:
            state.active = False
            state.severity = "none"
            state.cleared_at = _now()
            state.reasons = []
            state._quiet_polls = 0
    else:
        state.severity = "none"
        state.reasons = []

    return state
