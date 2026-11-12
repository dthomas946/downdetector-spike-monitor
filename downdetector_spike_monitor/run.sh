#!/usr/bin/with-contenv bashio
# Supervisor hands us MQTT broker details when the Mosquitto add-on is installed.
if bashio::services.available mqtt; then
  export MQTT_HOST="$(bashio::services mqtt 'host')"
  export MQTT_PORT="$(bashio::services mqtt 'port')"
  export MQTT_USER="$(bashio::services mqtt 'username')"
  export MQTT_PASSWORD="$(bashio::services mqtt 'password')"
  bashio::log.info "MQTT discovered at ${MQTT_HOST}:${MQTT_PORT}"
else
  bashio::log.warning "No MQTT service; entities will not be created (webhook still works)"
fi
export LOG_LEVEL="$(bashio::config 'log_level')"
export WATCHLIST_FILE=/data/watchlist.yaml
[ -f /data/watchlist.yaml ] || cp /app/watchlist.example.yaml /data/watchlist.yaml
cd /app && exec python -m uvicorn app.main:app --host 0.0.0.0 --port 8099
