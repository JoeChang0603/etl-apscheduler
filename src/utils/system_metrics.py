"""Collect process, system, and cgroup metrics for observability APIs."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import psutil

try:  # Docker SDK is optional but used when available.
    import docker
    from docker.errors import DockerException
except Exception:  # pragma: no cover - gracefully handle missing SDK
    docker = None
    DockerException = RuntimeError  # type: ignore[assignment]


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


def collect_resource_snapshot(device: str) -> Dict[str, Any]:
    """Return disk usage for ``device`` and Docker container metrics.

    :param device: Block device path or mount point to inspect.
    :return: Mapping describing disk utilisation and Docker stats.
    """

    disk_info = disk_usage(device)
    docker_info = docker_container_metrics()

    return {
        "collected_at": datetime.utcnow().isoformat() + "Z",
        "device": disk_info,
        "docker": docker_info,
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


def disk_usage(device: str) -> Dict[str, Any]:
    """Inspect disk utilisation for ``device`` or its mount point.

    :param device: Block device path (e.g. ``/dev/nvme0n1p1``) or mount point.
    :return: Disk usage statistics with availability flag.
    """

    mount_point = _resolve_mount_point(device)
    if mount_point is None:
        return {
            "available": False,
            "device": device,
            "error": "device_or_mount_not_found",
        }

    try:
        usage = psutil.disk_usage(mount_point)
    except (FileNotFoundError, PermissionError):
        return {
            "available": False,
            "device": device,
            "mount_point": mount_point,
            "error": "mount_point_unavailable",
        }

    return {
        "available": True,
        "device": device,
        "mount_point": mount_point,
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "percent": usage.percent,
    }


def docker_container_metrics() -> Dict[str, Any]:
    """Collect CPU, memory, and disk I/O metrics for running containers."""

    if docker is None:
        return {
            "available": False,
            "error": "docker_sdk_not_installed",
        }

    try:
        client = docker.DockerClient.from_env()
    except DockerException as exc:  # pragma: no cover - environment specific
        return {
            "available": False,
            "error": str(exc),
        }

    try:
        containers = client.containers.list()
    except DockerException as exc:
        client.close()
        return {
            "available": False,
            "error": str(exc),
        }

    containers_data = []
    try:
        for container in containers:
            try:
                stats = container.stats(stream=False)
                if not isinstance(stats, dict):
                    raise DockerException("invalid_stats_payload")
                metadata = _container_metrics_from_stats(container, stats)
            except DockerException as exc:  # pragma: no cover - per container failure
                metadata = {
                    "id": container.short_id,
                    "name": container.name,
                    "error": str(exc),
                }
            containers_data.append(metadata)
    finally:
        client.close()

    return {
        "available": True,
        "containers": containers_data,
    }


def _container_metrics_from_stats(container: Any, stats: Dict[str, Any]) -> Dict[str, Any]:
    """Project Docker stats output into a compact summary."""

    cpu_percent = _calculate_cpu_percent(stats)

    memory_stats = stats.get("memory_stats", {}) or {}
    mem_usage = memory_stats.get("usage")
    mem_limit = memory_stats.get("limit")
    mem_percent = None
    if mem_usage is not None and mem_limit:
        try:
            mem_percent = (mem_usage / mem_limit) * 100
        except ZeroDivisionError:
            mem_percent = None

    blkio = stats.get("blkio_stats", {}) or {}
    io_service = blkio.get("io_service_bytes_recursive") or []
    read_bytes = _sum_blkio(io_service, {"Read"})
    write_bytes = _sum_blkio(io_service, {"Write"})

    storage = stats.get("storage_stats", {}) or {}
    storage_usage = storage.get("size_rw")
    if storage_usage is None:
        storage_usage = storage.get("usage")

    pids = stats.get("pids_stats", {}).get("current")

    attrs = getattr(container, "attrs", {}) or {}
    state = attrs.get("State") or {}

    info = {
        "id": container.short_id,
        "name": container.name,
        "status": getattr(container, "status", None),
        "cpu_percent": cpu_percent,
        "memory": {
            "usage_bytes": mem_usage,
            "limit_bytes": mem_limit,
            "percent": mem_percent,
        },
        "block_io": {
            "read_bytes": read_bytes,
            "write_bytes": write_bytes,
        },
        "pids": pids,
    }

    if storage_usage is not None:
        info["storage_bytes"] = storage_usage

    started_at = None
    if isinstance(state, dict):
        started_at = state.get("StartedAt")
    if started_at:
        info["started_at"] = started_at

    return info


def _calculate_cpu_percent(stats: Dict[str, Any]) -> Optional[float]:
    cpu_stats = stats.get("cpu_stats") or {}
    precpu_stats = stats.get("precpu_stats") or {}

    cpu_total = _nested_get(cpu_stats, "cpu_usage", "total_usage")
    precpu_total = _nested_get(precpu_stats, "cpu_usage", "total_usage")
    system_usage = cpu_stats.get("system_cpu_usage")
    presystem_usage = precpu_stats.get("system_cpu_usage")

    if None in (cpu_total, precpu_total, system_usage, presystem_usage):
        return None

    cpu_delta = cpu_total - precpu_total
    system_delta = system_usage - presystem_usage
    if cpu_delta <= 0 or system_delta <= 0:
        return None

    percpu_usage = _nested_get(cpu_stats, "cpu_usage", "percpu_usage") or []
    cpu_count = len(percpu_usage) or 1

    return (cpu_delta / system_delta) * cpu_count * 100.0


def _sum_blkio(records: Iterable[Dict[str, Any]], operations: set[str]) -> Optional[int]:
    total = 0
    matched = False
    for record in records:
        op = record.get("op")
        if op not in operations:
            continue
        value = record.get("value")
        if value is None:
            continue
        matched = True
        total += int(value)
    return total if matched else None


def _nested_get(data: Dict[str, Any], *keys: str) -> Optional[Any]:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
        if current is None:
            return None
    return current


def _resolve_mount_point(device: str) -> Optional[str]:
    """Return a mount point for ``device`` or validate a path."""

    path = Path(device)
    if path.is_dir():
        return str(path)

    try:
        partitions = psutil.disk_partitions(all=True)
    except PermissionError:
        return None

    for partition in partitions:
        if partition.device == device:
            return partition.mountpoint
    return None


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
