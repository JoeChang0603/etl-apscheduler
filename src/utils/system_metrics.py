"""Collect process, system, and cgroup metrics for observability APIs."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import psutil


CGROUP_ROOT = Path("/sys/fs/cgroup")


def collect_metrics() -> Dict[str, Any]:
    """Gather process, system, and cgroup metrics for the running container.

    :return: Dictionary containing process, system, and cgroup metrics.
    """

    process_info = _process_metrics()
    system_info = _system_metrics()
    cgroup_info = _cgroup_metrics()

    return {
        "collected_at": datetime.utcnow().isoformat() + "Z",
        "process": process_info,
        "system": system_info,
        "cgroup": cgroup_info,
    }


def _process_metrics() -> Dict[str, Any]:
    """Collect metrics for the current Python process.

    :return: Process metrics such as CPU, memory, threads, and command line.
    """
    proc = psutil.Process(os.getpid())
    with proc.oneshot():
        cpu_percent = proc.cpu_percent(interval=None)
        mem_info = proc.memory_info()
        mem_percent = proc.memory_percent()
        threads = proc.num_threads()
        open_files = len(proc.open_files())
        cmdline = proc.cmdline()
        create_time = datetime.utcfromtimestamp(proc.create_time()).isoformat() + "Z"

    return {
        "pid": proc.pid,
        "cpu_percent": cpu_percent,
        "memory_bytes": mem_info.rss,
        "memory_percent": mem_percent,
        "threads": threads,
        "open_files": open_files,
        "cmdline": cmdline,
        "create_time": create_time,
    }


def _system_metrics() -> Dict[str, Any]:
    """Collect host-wide CPU and memory statistics.

    :return: System metrics including CPU usage and memory/swap data.
    """
    cpu_percent = psutil.cpu_percent(interval=None)
    load_avg: Optional[tuple[float, float, float]] = None
    try:
        load_avg = os.getloadavg()
    except (AttributeError, OSError):
        load_avg = None

    virtual_memory = psutil.virtual_memory()
    swap = psutil.swap_memory()

    return {
        "cpu_percent": cpu_percent,
        "load_average": load_avg,
        "memory_total": virtual_memory.total,
        "memory_available": virtual_memory.available,
        "memory_used": virtual_memory.used,
        "memory_percent": virtual_memory.percent,
        "swap_total": swap.total,
        "swap_used": swap.used,
        "swap_percent": swap.percent,
    }


def _cgroup_metrics() -> Dict[str, Any]:
    """Collect cgroup-specific metrics for containerised environments.

    :return: Dictionary describing memory and CPU limits where available.
    """
    if not CGROUP_ROOT.exists():
        return {"available": False}

    data: Dict[str, Any] = {"available": True}

    memory_current = _read_int(CGROUP_ROOT / "memory.current")
    memory_max_raw = _read_text(CGROUP_ROOT / "memory.max")
    memory_max = None if memory_max_raw in ("" , "max") else int(memory_max_raw)

    cpu_max_raw = _read_text(CGROUP_ROOT / "cpu.max")
    cpu_quota = None
    cpu_period = None
    if cpu_max_raw and " " in cpu_max_raw:
        quota_str, period_str = cpu_max_raw.split()
        cpu_quota = None if quota_str == "max" else int(quota_str)
        cpu_period = int(period_str)

    cpu_stat = _read_key_value(CGROUP_ROOT / "cpu.stat")

    data.update(
        {
            "memory_current": memory_current,
            "memory_max": memory_max,
            "cpu_quota": cpu_quota,
            "cpu_period": cpu_period,
            "cpu_stat": cpu_stat,
        }
    )
    return data


def _read_text(path: Path) -> Optional[str]:
    """Safely read a text file, returning ``None`` on access errors.

    :param path: File path to read.
    :return: File contents without trailing whitespace, or ``None``.
    """
    try:
        return path.read_text().strip()
    except FileNotFoundError:
        return None
    except PermissionError:
        return None


def _read_int(path: Path) -> Optional[int]:
    """Read an integer value from ``path`` where possible.

    :param path: File path expected to contain an integer.
    :return: Parsed integer value, or ``None`` if parsing fails.
    """
    text = _read_text(path)
    if text is None:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _read_key_value(path: Path) -> Dict[str, Any]:
    """Parse ``key value`` formatted text files into a dictionary.

    :param path: File path to parse.
    :return: Mapping of keys to integer or string values.
    """
    text = _read_text(path)
    if not text:
        return {}

    data: Dict[str, Any] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) != 2:
            continue
        key, value = parts
        try:
            data[key] = int(value)
        except ValueError:
            data[key] = value
    return data
