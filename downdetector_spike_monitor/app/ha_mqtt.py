"""
Home Assistant delivery via MQTT discovery.

For every watched target we publish one HA device with:
  binary_sensor.<slug>_outage_spike   (problem class, on while a spike is active)
  sensor.<slug>_reports_15m           (current 15-min report count)
  sensor.<slug>_baseline              (expected count)
  sensor.<slug>_downdetector_status   (success/warning/danger)

Plus a global  event  topic  downdetector/events  that fires on spike start/clear,
which you can use as an MQTT trigger in automations.

Optionally also POSTs to an HA webhook (no MQTT needed).
"""
from __future__ import annotations

import json
import logging
import threading
from typing import Optional

import httpx

from .spike import SpikeState
from .watchlist import Target

log = logging.getLogger("dd.ha")

try:
    import paho.mqtt.client as mqtt
except ImportError:  # pragma: no cover
    mqtt = None


class HAPublisher:
    def __init__(self, mqtt_host: str = "", mqtt_port: int = 1883, mqtt_user: str = "",
                 mqtt_password: str = "", discovery_prefix: str = "homeassistant",
                 base_topic: str = "downdetector", webhook_url: str = ""):
        self.base = base_topic
        self.disc = discovery_prefix
        self.webhook_url = webhook_url
        self._announced: set[str] = set()
        self._client = None
        self._lock = threading.Lock()
        if mqtt_host and mqtt:
            c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="downdetector-spike-monitor")
            if mqtt_user:
                c.username_pw_set(mqtt_user, mqtt_password)
            c.will_set(f"{self.base}/availability", "offline", retain=True)
            c.connect_async(mqtt_host, mqtt_port, keepalive=60)
            c.loop_start()
            self._client = c
            log.info("MQTT publisher -> %s:%s", mqtt_host, mqtt_port)
        elif mqtt_host and not mqtt:
            log.error("paho-mqtt not installed; MQTT disabled")

    # ----------------------------------------------------------------- mqtt
    def _pub(self, topic: str, payload, retain: bool = False):
        if not self._client:
            return
        if not isinstance(payload, str):
            payload = json.dumps(payload)
        self._client.publish(topic, payload, qos=1, retain=retain)

    def online(self):
        self._pub(f"{self.base}/availability", "online", retain=True)

    def _announce(self, t: Target):
        uid = t.uid.replace("@", "_").replace("-", "_")
        if uid in self._announced:
            return
        device = {
            "identifiers": [f"downdetector_{uid}"],
            "name": f"Downdetector {t.name}" + (f" ({t.city})" if t.city else ""),
            "manufacturer": "Downdetector",
            "model": "Spike Monitor",
            "configuration_url": t.url or "https://downdetector.com",
        }
        state_topic = f"{self.base}/{uid}/state"
        common = {"device": device, "availability_topic": f"{self.base}/availability",
                  "state_topic": state_topic}
        comps = {
            ("binary_sensor", "outage_spike"): {
                "name": "Outage spike", "device_class": "problem",
                "value_template": "{{ 'ON' if value_json.active else 'OFF' }}",
                "json_attributes_topic": state_topic,
            },
            ("sensor", "reports_15m"): {
                "name": "Reports (15m)", "state_class": "measurement", "icon": "mdi:alert-circle-outline",
                "value_template": "{{ value_json.current }}",
            },
            ("sensor", "baseline"): {
                "name": "Baseline", "state_class": "measurement", "icon": "mdi:chart-line",
                "value_template": "{{ value_json.baseline }}",
            },
            ("sensor", "downdetector_status"): {
                "name": "Downdetector status", "icon": "mdi:web",
                "value_template": "{{ value_json.dd_status or 'unknown' }}",
            },
        }
        for (comp, key), cfg in comps.items():
            cfg.update(common)
            cfg["unique_id"] = f"downdetector_{uid}_{key}"
            self._pub(f"{self.disc}/{comp}/downdetector_{uid}/{key}/config", cfg, retain=True)
        self._announced.add(uid)

    def publish_state(self, t: Target, s: SpikeState):
        self._announce(t)
        uid = t.uid.replace("@", "_").replace("-", "_")
        self._pub(f"{self.base}/{uid}/state", s.to_dict(), retain=True)

    # -------------------------------------------------------- national scan
    def publish_national(self, summ: dict):
        iso = (summ.get("country") or "US").lower()
        uid = f"national_{iso}"
        state_topic = f"{self.base}/{uid}/state"
        if uid not in self._announced:
            device = {"identifiers": [f"downdetector_{uid}"],
                      "name": f"Downdetector National ({iso.upper()})",
                      "manufacturer": "Downdetector", "model": "National scan"}
            common = {"device": device, "availability_topic": f"{self.base}/availability",
                      "state_topic": state_topic}
            comps = {
                ("binary_sensor", "widespread_outage"): {
                    "name": "Widespread outage", "device_class": "problem",
                    "value_template": "{{ 'ON' if value_json.widespread else 'OFF' }}",
                    "json_attributes_topic": state_topic,
                    "json_attributes_template": "{{ {'affected': value_json.affected, 'cloudflare': value_json.cloudflare, 'since': value_json.widespread_since, 'scanned_at': value_json.scanned_at} | tojson }}",
                },
                ("sensor", "services_in_danger"): {
                    "name": "Services in danger", "state_class": "measurement", "icon": "mdi:alert",
                    "value_template": "{{ value_json.services_in_danger }}"},
                ("sensor", "top_service"): {
                    "name": "Most reported service", "icon": "mdi:podium-gold",
                    "value_template": "{{ value_json.top_service or 'none' }}"},
                ("sensor", "top_service_reports"): {
                    "name": "Most reported service reports", "state_class": "measurement",
                    "icon": "mdi:counter", "value_template": "{{ value_json.top_service_reports }}"},
                ("sensor", "cloudflare_status"): {
                    "name": "Cloudflare status (at last widespread check)", "icon": "mdi:cloud-alert",
                    "value_template": "{{ (value_json.cloudflare or {}).get('indicator', 'not checked') }}"},
            }
            for (comp, key), cfg in comps.items():
                cfg.update(common)
                cfg["unique_id"] = f"downdetector_{uid}_{key}"
                self._pub(f"{self.disc}/{comp}/downdetector_{uid}/{key}/config", cfg, retain=True)
            self._announced.add(uid)
        self._pub(state_topic, summ, retain=True)

    def publish_raw_event(self, payload: dict):
        self._pub(f"{self.base}/events", payload)
        if self.webhook_url:
            try:
                httpx.post(self.webhook_url, json=payload, timeout=10)
            except Exception as e:  # noqa: BLE001
                log.warning("webhook post failed: %s", e)
        log.info("EVENT %s %s", payload.get("event"),
                 {k: payload.get(k) for k in ("country", "services_in_danger", "top_service")})

    # --------------------------------------------------------------- events
    def publish_event(self, t: Target, s: SpikeState, kind: str):
        """kind: 'spike_started' | 'spike_cleared' | 'spike_escalated'"""
        payload = {"event": kind, "target": t.model_dump(), **s.to_dict()}
        self._pub(f"{self.base}/events", payload)
        if self.webhook_url:
            try:
                httpx.post(self.webhook_url, json=payload, timeout=10)
            except Exception as e:  # noqa: BLE001
                log.warning("webhook post failed: %s", e)
        log.info("EVENT %s %s current=%s baseline=%s reasons=%s",
                 kind, t.uid, s.current, s.baseline, s.reasons)
