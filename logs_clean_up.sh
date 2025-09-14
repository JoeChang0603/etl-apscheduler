#!/usr/bin/env bash
set -euo pipefail

DIR="/Users/joechang/Project/Kairos/data-warehouse-apscheduler/logs"
RETENTION_DAYS=14
SAFE_MIN=10   # 近 10 分鐘內修改過的檔案不刪，避免正在寫入

# GNU find（大多數標準 Linux）
find "$DIR" -type f \
  \( -name "*.log" -o -name "*.log.gz" -o -name "*.error.log" \) \
  -mmin +$SAFE_MIN -mtime +$RETENTION_DAYS -print -delete
