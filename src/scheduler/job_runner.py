"""Helpers that resolve and execute job callables with logging instrumentation."""

import inspect
import importlib
from typing import Any, Callable, Dict

from src.utils.logger.config import LogLevel
from src.utils.logger_factory import EnhancedLoggerFactory, log_exception


def resolve_jobs_callable(spec: str) -> Callable:
    """Translate a job spec string into an async callable under ``src.jobs``.

    :param spec: Job function spec such as ``price_poller`` or ``pkg.mod:func``.
    :return: Coroutine function resolved from ``src.jobs``.
    :raises TypeError: If the resolved callable is not async.
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

async def run_job(
    func_spec: str,
    job_id: str,
    kwargs: Dict[str, Any],
    *,
    level: LogLevel = LogLevel.DEBUG,
) -> None:
    """Resolve a job target, inject logging, and execute it for APScheduler.

    :param func_spec: Job function spec string passed from APScheduler.
    :param job_id: Identifier of the APScheduler job being executed.
    :param kwargs: Keyword arguments to forward to the resolved job coroutine.
    :param level: Minimum log level for the per-run logger.
    :raises TypeError: If ``func_spec`` does not resolve to an async callable.
    :raises Exception: Re-raises execution errors after logging them.
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
