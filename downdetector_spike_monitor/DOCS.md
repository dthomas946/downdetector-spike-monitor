# Downdetector Spike Monitor

Polls Downdetector for the companies, websites, countries and cities on your
watchlist and raises a Home Assistant `binary_sensor` (device class *problem*)
whenever user-reported issues spike well above the normal baseline.

## Setup
1. Install the **Mosquitto broker** add-on and the **MQTT** integration (entities
   are created by MQTT discovery). Skip this if you only want webhook events.
2. The default `backend: unofficial` reads the public downdetector.com status pages —
   no account needed. If polls log a Cloudflare challenge, install a FlareSolverr
   add-on and set `flaresolverr_url` (e.g. `http://<addon-host>:8191/v1`). Enterprise
   API users can set `backend: enterprise` with their Client ID / Secret instead.
3. Start the add-on. Edit `/share/../data/watchlist.yaml` or use the REST API on
   port 8099 (`POST /watchlist`).

## Entities per watched company
- `binary_sensor.downdetector_<slug>_outage_spike`
- `sensor.downdetector_<slug>_reports_15m`, `_baseline`, `_downdetector_status`

## Automation example
```yaml
alias: Downdetector spike notify
trigger:
  - platform: mqtt
    topic: downdetector/events
condition: "{{ trigger.payload_json.event == 'spike_started' }}"
action:
  - service: notify.mobile_app_phone
    data:
      title: "{{ trigger.payload_json.name }} outage reports spiking"
      message: >
        {{ trigger.payload_json.current }} reports vs baseline
        {{ trigger.payload_json.baseline }} — {{ trigger.payload_json.reasons | join(', ') }}
```
