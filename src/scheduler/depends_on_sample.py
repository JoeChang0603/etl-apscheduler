# src/scheduler/scheduler.py（或你現有檔案）
# from collections import defaultdict
# from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
# from apscheduler.triggers.date import DateTrigger
# from datetime import datetime, timezone
# from src.scheduler.job_runner import run_job

# UTC = timezone.utc

# def load_jobs_from_yaml(scheduler, path, tz=UTC, etl_logger=None):
#     log = etl_logger or logger
#     data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}

#     templates = {}       # child_id -> {func, kwargs, log_prefix, ...}
#     reverse_deps = defaultdict(list)  # parent_id -> [child_id, ...]

#     for item in data.get("jobs", []):
#         job_id   = item["id"]
#         func_spec= item["func"]
#         kwargs   = item.get("kwargs", {}) or {}
#         log_prefix = item.get("log_prefix")
#         trigger_type = item.get("trigger", "cron")
#         depends_on = item.get("depends_on")  # 可支援字串或陣列

#         if depends_on:
#             # 註冊成「樣板」，等待父任務成功時動態觸發
#             templates[job_id] = {"func": func_spec, "kwargs": kwargs, "log_prefix": log_prefix}
#             parents = [depends_on] if isinstance(depends_on, str) else list(depends_on)
#             for p in parents:
#                 reverse_deps[p].append(job_id)
#             log.info(f"Registered dependent template: {job_id} depends_on {parents}")
#             continue

#         # === 沒有依賴的 job：照常加進 Scheduler（用固定 Runner） ===
#         kwargs.pop("logger", None)  # 防止 YAML 中意外提供 logger
#         trigger = _build_trigger_from_item(item, tz)  # 你既有的 trigger 建立邏輯
#         scheduler.add_job(
#             func=run_job,
#             trigger=trigger,
#             kwargs={"func_spec": func_spec, "job_id": job_id, "kwargs": kwargs, "log_prefix": log_prefix},
#             id=job_id,
#             replace_existing=True,
#             **DEFAULTS,
#         )
#         log.info(f"Registered job: {job_id} ({trigger_type}) -> {func_spec}")

#     # === 安裝 listener：A 成功 -> 觸發 B（一次性 Date 觸發） ===
#     def _on_event(event):
#         if event.code == EVENT_JOB_EXECUTED:
#             parent = event.job_id
#             for child in reverse_deps.get(parent, []):
#                 t = templates[child]
#                 # 這裡用「一次性」任務，避免與 child 的固定排程衝突
#                 run_id = f"{child}__dep__{int(datetime.now(UTC).timestamp()*1e6)}"
#                 try:
#                     scheduler.add_job(
#                         func=run_job,
#                         trigger=DateTrigger(run_date=datetime.now(UTC), timezone=tz),
#                         kwargs={"func_spec": t["func"], "job_id": child, "kwargs": t.get("kwargs", {}), "log_prefix": t.get("log_prefix")},
#                         id=run_id,  # 唯一 id，不覆蓋
#                         replace_existing=False,
#                         **DEFAULTS,
#                     )
#                     log.info(f"Dependency satisfied: {parent} -> scheduled {child} (one-off id={run_id})")
#                 except Exception as e:
#                     log.exception(f"Failed to schedule dependent job {child} after {parent}: {e}")
#         elif event.code == EVENT_JOB_ERROR:
#             log.error(f"Parent job failed: {event.job_id}; dependents will not be triggered")

#     scheduler.add_listener(_on_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)


#     jobs:
#   - id: fetch_price
#     func: price_poller  # 會解析成 src.jobs.price_poller:run
#     trigger: interval
#     every: { seconds: 5 }
#     kwargs:
#       symbol: "BTCUSDT"
#       interval: 0.5

#   - id: enrich_price
#     func: price_enricher  # 例如 src.jobs.price_enricher:run
#     depends_on: fetch_price   # 或 [fetch_price, another_job]
#     kwargs:
#       # 你的參數...
