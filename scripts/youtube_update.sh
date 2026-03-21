#!/bin/bash
# YouTube チャンネル自動更新スクリプト
# 全3チャンネルの新着動画をチェック → Whisper文字起こし → DB格納
#
# 手動実行: bash scripts/youtube_update.sh
# 自動実行: launchd plist (毎日6:00 AM)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="/tmp/youtube_update_logs"
LOG_FILE="$LOG_DIR/update_$(date +%Y%m%d_%H%M%S).log"

mkdir -p "$LOG_DIR"

echo "=== YouTube Auto-Update Started: $(date) ===" | tee "$LOG_FILE"

cd "$PROJECT_DIR"
PYTHONUNBUFFERED=1 python3 scripts/ingest_youtube_channel.py \
    --channel all --update 2>&1 | tee -a "$LOG_FILE"

echo "=== Finished: $(date) ===" | tee -a "$LOG_FILE"

# Keep only last 30 log files
ls -t "$LOG_DIR"/update_*.log 2>/dev/null | tail -n +31 | xargs rm -f 2>/dev/null
