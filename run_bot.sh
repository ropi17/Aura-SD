#!/bin/bash
# Watchdog: jalankan aura_bot, restart otomatis kalau mati (proxy VM ini flaky).
cd "$(dirname "$0")"
export RUST_LOG=info
while true; do
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Menjalankan aura_bot..." >> watchdog.log
  ./target/release/aura_bot >> bot.log 2>&1
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] aura_bot berhenti (exit $?). Restart dalam 5 detik..." >> watchdog.log
  sleep 5
done
