# src/scheduler/job_runner.py
import inspect
import importlib
from typing import Dict, Any, Callable

from src.utils.logger_factory import EnhancedLoggerFactory, log_exception
from src.utils.logger.config import LogLevel

def resolve_jobs_callable(spec: str) -> Callable:
    """
    支援：
      - "price_poller"           -> src.jobs.price_poller:run
      - "rebalance:run_once"     -> src.jobs.rebalance:run_once
      - "src.jobs.x.y:func"      -> 已是完整路徑，原樣解析
    """
    if ":" in spec:
        module_part, func_name = spec.split(":", 1)
    else:
        module_part, func_name = spec, "run"

    if module_part.startswith("src.jobs."):
        module_path = module_part
    else:
        module_path = f"src.jobs.{module_part}"

    mod = importlib.import_module(module_path)
    fn = getattr(mod, func_name)
    if not inspect.iscoroutinefunction(fn):
        raise TypeError(f"Job function must be async: {module_path}:{func_name}")
    return fn

async def run_job(func_spec: str, job_id: str, kwargs: Dict[str, Any], *,
                  level: LogLevel = LogLevel.DEBUG) -> None:
    """
    固定的 Runner：由 APScheduler 直接呼叫。
    於執行時才解析 func_spec -> 真正 callable，並注入 logger。
    """
    fn = resolve_jobs_callable(func_spec)  # 例如 "price_poller" 或 "src.jobs.price_poller:run"

    # 僅在目標函式接受 logger 或 **kwargs 時才注入
    sig = inspect.signature(fn)
    accepts_logger = ("logger" in sig.parameters) or any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
    )

    async with EnhancedLoggerFactory.job_run_logger(
        job_id=job_id, level=level
    ) as log:
        try:
            call_kwargs = dict(kwargs or {})
            if accepts_logger:
                call_kwargs.setdefault("logger", log)
                log.info(f"logger injected (id={id(log)}) into {fn.__module__}.{fn.__qualname__}")
            else:
                log.warning(f"Target '{fn.__module__}.{fn.__qualname__}' has no 'logger' kwarg")

            # 呼叫目標 job
            return await fn(**call_kwargs)
        except Exception as e:
            log_exception(log, e, context=f"job:{job_id}")
            raise
