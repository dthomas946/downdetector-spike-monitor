# Downdetector Spike Monitor

A small service that polls Downdetector for the companies, websites, countries and
cities you care about, detects **spikes in user-reported problems**, exposes a REST
API, and pushes alerts into **Home Assistant** (MQTT discovery entities + an event
topic, or a webhook). Ships as a Home Assistant add-on.

```
 watchlist.yaml / REST ──► resolve ──► poll backend ──► spike detector ──► MQTT / webhook ──► HA
   company / url /            │          (enterprise API         (ratio, z-score,
   country / city             │           or unofficial scrape)   DD status, hysteresis)
                              └──► GET /status  /spikes  /search
```

## Layout (this repo is a Home Assistant add-on repository)
```
repository.yaml                     add-on repo manifest (add this repo URL in the Add-on Store)
downdetector_spike_monitor/         the add-on
  config.yaml  Dockerfile  run.sh   HA add-on packaging
  app/
    dd_client.py   Downdetector Enterprise API v2 client (OAuth client-credentials)
    backends.py    UnofficialBackend (public page scrape, default) + EnterpriseBackend
    spike.py       spike rules + hysteresis
    watchlist.py   YAML-backed watchlist: company | url | country | city
    monitor.py     polling loop + state
    ha_mqtt.py     HA MQTT discovery + events + webhook
    main.py        FastAPI REST API / entrypoint
  requirements.txt  watchlist.example.yaml
tests/                              pytest suite + a fake Downdetector server
docker-compose.yml  Dockerfile.standalone   run it outside Home Assistant
```

## Data source: two backends

| backend      | needs | company | url | country | city |
|--------------|-------|:-------:|:---:|:-------:|:----:|
| `unofficial` (default) | nothing | ✅ | ✅ | ✅ (every company in the site's `/companies/` directory) | national only* |
| `enterprise` | Downdetector Enterprise Client ID/Secret | ✅ | ✅ | ✅ | ✅ (`/reports?latitude&longitude`) |

### `unofficial` — no credentials (verified against the live site 2026-11-12)
downdetector.com is now a Next.js app; every `/status/<slug>/` page server-renders the
same numbers the Enterprise API returns: `stats.status` (`success|warning|danger`),
96 fifteen-minute points of `{timestampUtc, reportsValue, baselineValue}`, the top
problem indicators (e.g. *EC2*, *us-east-1*) and the top reporting cities. The
backend parses that payload directly — no JavaScript execution, no headless browser.
`/companies/` lists all ~2,900 US slugs, which is what powers `/search`, URL→company
matching, and `kind: country`.

**Cloudflare.** The site sits behind Cloudflare. The fetcher tries, in order:
1. `curl_cffi` with a Chrome TLS fingerprint (installed by default) — usually enough;
2. plain `httpx` with a browser User-Agent (if curl_cffi is missing);
3. **FlareSolverr** if `FLARESOLVERR_URL` is set (e.g. `http://flaresolverr:8191/v1`) —
   a self-hosted browser that solves challenges; there are community HA add-ons for it.

If polls log `Cloudflare challenge`, raise `poll_interval` and/or set up FlareSolverr.
\* The page lists the top ~10 reporting cities; those are exposed as a sensor attribute,
but per-city report counts need the enterprise `/reports` endpoint.

Country sites (downdetector.co.uk, .ca, …) run the same app and are handled by
`country:` on a watch entry, mapped in `COUNTRY_SITE_DOMAINS`.

### `enterprise` — official API
Auth is `POST /v2/tokens?grant_type=client_credentials` with HTTP Basic
(Client ID / Secret), then `Authorization: Bearer …`. One bulk `GET /v2/companies?ids=…`
per poll covers every watched company.

## National scan & widespread outages
Add a `country` entry (e.g. `US`). Instead of polling ~2,900 company pages, the add-on
fetches that country's Downdetector **homepage once an hour at HH:`national_scan_minute`**
(default :00). The homepage lists the ~47 most-reported services nationwide, each with
Downdetector's status and a 24 h report sparkline — any service with a real outage shows up there.

A **widespread outage** is declared when ≥ `widespread_min_services` (3) services are at
`danger` with ≥ `widespread_min_reports` (1,000) reports in a 15-min bucket within the last hour.
When that happens (and on each hourly scan while it lasts) the add-on checks
**Cloudflare's public status API** and reports whether Cloudflare has a major incident —
i.e. whether it's the likely common cause. Events `widespread_outage_started` /
`widespread_outage_cleared` go to `downdetector/events`; entities appear under the device
*Downdetector National (US)*. `POST /scan` (or **Scan now** in the dashboard) runs a scan immediately.

Watched companies whose own status page is blocked fall back to the national scan's numbers
whenever they appear in it.

**FlareSolverr** (`flaresolverr_url`) is used for the homepage and the company directory; it
gets those through Cloudflare reliably. Individual status pages are fetched directly.

## Dashboard
Open **Downdetector** in the Home Assistant sidebar (Ingress panel), or `http://<HA>:8099/` directly.
It shows active spikes, a 24-hour reports-vs-baseline chart and top reported problems per
service, and lets you add/remove watchlist entries and search Downdetector companies.

## Spike rules (`/config` shows the live values)
A target is *spiking* when any of these is true, and **current ≥ `spike_min_reports`**
for the numeric rules so a 1→6 blip on a quiet service never pages you:

| rule | default |
|---|---|
| Downdetector's own `status == danger` (`warning` too if `spike_warn_on_warning`) | on |
| current ≥ `spike_ratio` × baseline_current | 3× |
| z-score vs the previous 12 fifteen-minute buckets ≥ `spike_z` | 3.0 |
| hysteresis: clears only after `spike_clear_after` consecutive polls below 50 % of the trigger level | 2 |

Severity is `danger` when DD says danger or the ratio is ≥ 2× the threshold, else `warning`.

## Run standalone
```bash
pip install -r downdetector_spike_monitor/requirements.txt
cd downdetector_spike_monitor
export DD_CLIENT_ID=… DD_CLIENT_SECRET=…
export WATCHLIST_FILE=./watchlist.yaml  OPTIONS_FILE=/nonexistent
export MQTT_HOST=homeassistant.local MQTT_USER=mqtt MQTT_PASSWORD=…   # optional
uvicorn app.main:app --host 0.0.0.0 --port 8099
```
or `docker compose up` (see `docker-compose.yml`). Default is `BACKEND=unofficial`; set
`BACKEND=enterprise` plus `DD_CLIENT_ID/SECRET` if you have an Enterprise account.

Try it without any Downdetector account: `python tests/fake_server.py` in one shell,
then `DD_BASE_URL=http://127.0.0.1:8555/v2 DD_CLIENT_ID=x DD_CLIENT_SECRET=x …` —
the fake makes AWS "go down" five seconds after it starts.

## REST API (port 8099)
| method | path | purpose |
|---|---|---|
| GET | `/health` | liveness, last poll, last error |
| GET | `/status` | all targets' state + active spikes |
| GET | `/spikes` | active spikes only |
| GET | `/companies/{uid}` | one target (`aws`, `att@Little Rock`, …) |
| GET | `/search?name=&country=` | find Downdetector companies/slugs to add |
| GET | `/watchlist` | current entries |
| POST | `/watchlist` | add `{"kind":"company|url|country|city","value":…,"country":"US","latitude":…,"longitude":…,"companies":[…]}` |
| DELETE | `/watchlist/{key}` | remove by `kind:value:country` |
| POST | `/poll` | poll now |
| GET | `/config` | effective thresholds |

## Home Assistant
**Add-on (the "marketplace" path):** push this repo to GitHub, then in HA go to
*Settings → Add-ons → Add-on store → ⋮ → Repositories* and add the repo URL. Supervisor builds the image from `downdetector_spike_monitor/Dockerfile`. Mosquitto broker credentials are
picked up automatically via `services: mqtt:want`.

**Entities** (via MQTT discovery, one device per watched company):
`binary_sensor.downdetector_<slug>_outage_spike` (device class *problem*, attributes
carry ratio/z-score/reasons), `sensor…_reports_15m`, `sensor…_baseline`,
`sensor…_downdetector_status`.

**Events:** every spike start / escalation / clear is published to `downdetector/events`
and POSTed to `ha_webhook_url` if set — see `downdetector_spike_monitor/DOCS.md` for an automation.

**Going further toward a proper HACS integration:** the add-on route needs no custom
component and works today. If you later want a native `custom_components/downdetector`
integration (config flow UI for adding countries/cities/URLs, no MQTT), `dd_client.py`,
`spike.py` and `watchlist.py` port over unchanged as the coordinator's data layer.

## Tests
```bash
python -m pytest tests -q
```
