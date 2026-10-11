"""Container cgroup metrics; do not mislabel process RSS as Koyeb usage."""
from __future__ import annotations

import os
import time
from pathlib import Path


def human_size(count: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if count < 1024 or unit == "GiB":
            return f"{count:.1f} {unit}" if unit != "B" else f"{count:.0f} B"
        count /= 1024
    return "0 B"


class ContainerMetrics:
    def __init__(self, root: Path = Path("/sys/fs/cgroup")):
        self.root = root
        self.last_wall = time.monotonic()
        self.last_cpu = self._cpu_usage()
        self.cached_cpu = "—"

    def _read(self, name: str) -> str | None:
        try:
            return (self.root / name).read_text().strip()
        except OSError:
            return None

    def _cpu_usage(self) -> float | None:
        stat = self._read("cpu.stat")
        if stat:
            for line in stat.splitlines():
                if line.startswith("usage_usec "):
                    return int(line.split()[1]) / 1_000_000
        old = self._read("cpuacct/cpuacct.usage")
        return int(old) / 1_000_000_000 if old else None

    def _cpu_quota(self) -> float | None:
        quota = self._read("cpu.max")
        if quota:
            limit, period = quota.split()[:2]
            return int(limit) / int(period) if limit != "max" else None
        old_quota = self._read("cpu/cpu.cfs_quota_us")
        old_period = self._read("cpu/cpu.cfs_period_us")
        if old_quota and old_period and int(old_quota) > 0:
            return int(old_quota) / int(old_period)
        return None

    def describe(self) -> str:
        now = time.monotonic()
        usage = self._cpu_usage()
        if usage is not None and self.last_cpu is not None and now - self.last_wall >= 5:
            percent = max(0, (usage - self.last_cpu) / (now - self.last_wall) * 100)
            quota = self._cpu_quota()
            self.cached_cpu = f"{percent / quota:.0f}% of quota" if quota else f"{percent:.0f}% of one core"
            self.last_cpu, self.last_wall = usage, now
        memory = self._read("memory.current") or self._read("memory/memory.usage_in_bytes")
        limit = self._read("memory.max") or self._read("memory/memory.limit_in_bytes")
        if memory is not None:
            total = int(limit) if limit and limit != "max" else 0
            if total >= 1 << 50:  # cgroup's sentinel for no configured limit
                total = 0
            mem = human_size(int(memory)) + (" / " + human_size(total) if total else "")
            return f"Container CPU: {self.cached_cpu} · RAM: {mem}"
        # Not running in a Linux cgroup (e.g. local development).
        return "Container CPU/RAM: unavailable"
