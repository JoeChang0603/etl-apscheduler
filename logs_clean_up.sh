set -euo pipefail

DIR="/home/admin/data-warehouse-apscheduler/logs"
RETENTION_DAYS=7
SAFE_MIN=10

echo "🧹 Cleaning host logs under $DIR (older than $RETENTION_DAYS days)..."
find "$DIR" -type f \
  \( -name "*.log" -o -name "*.log.gz" -o -name "*.error.log" \) \
  -mmin +$SAFE_MIN -mtime +$RETENTION_DAYS -print -delete

echo "🧹 Cleaning PostgreSQL logs inside Docker container 'etl_postgres'..."
docker exec -it etl_postgres bash -lc \
  "echo 'Inside container:' && \
   find /var/lib/postgresql/data/pg_log -type f -name '*.log' -mtime +$RETENTION_DAYS -print -delete"

echo "✅ Cleanup complete!"