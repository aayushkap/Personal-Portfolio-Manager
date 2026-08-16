# app/services/system.py
"""
SystemModule
Reads live host machine metrics (CPU, RAM, disk, battery/power, network,
temperature, top processes) for the "Data Center" system health dashboard.

No Cache/DB dependency — this reads directly from the OS via psutil,
so unlike other modules it's instantiated with no constructor args.

IMPORTANT: psutil's cpu_percent() and Process.cpu_percent() both work by
comparing against a *previous* sample. The very first call in a process's
life always returns 0 with no prior baseline. We warm this up once in
__init__ (called once at app startup, since deps.py wraps this in
@lru_cache) so every real request after that returns a true delta since
the last call.
"""

from __future__ import annotations

import platform
import socket
from datetime import datetime, timezone

import psutil


class SystemModule:
    def __init__(self):
        # Prime the baseline — first call always returns 0/garbage.
        psutil.cpu_percent(interval=None)
        psutil.cpu_percent(interval=None, percpu=True)
        for p in psutil.process_iter():
            try:
                p.cpu_percent(interval=None)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

    def get_health(self) -> dict:
        now = datetime.now(timezone.utc)
        boot_time = datetime.fromtimestamp(psutil.boot_time(), tz=timezone.utc)
        uptime_seconds = (now - boot_time).total_seconds()

        return {
            "timestamp": now.isoformat(),
            "host": self._get_host_info(boot_time, uptime_seconds),
            "cpu": self._get_cpu(),
            "memory": self._get_memory(),
            "swap": self._get_swap(),
            "disks": self._get_disks(),
            "disk_io": self._get_disk_io(),
            "network": self._get_network(),
            "power": self._get_power(),
            "temperatures": self._get_temperatures(),
            "top_processes": self._get_top_processes(limit=8),
        }

    # host
    def _get_host_info(self, boot_time: datetime, uptime_seconds: float) -> dict:
        try:
            load1, load5, load15 = psutil.getloadavg()
        except (AttributeError, OSError):
            load1 = load5 = load15 = None

        return {
            "hostname": socket.gethostname(),
            "os": f"{platform.system()} {platform.release()}",
            "architecture": platform.machine(),
            "boot_time": boot_time.isoformat(),
            "uptime_seconds": round(uptime_seconds),
            "uptime_human": self._humanize_seconds(uptime_seconds),
            "load_avg": {"1m": load1, "5m": load5, "15m": load15},
            "users_logged_in": len({u.name for u in psutil.users()}),
        }

    # cpu
    def _get_cpu(self) -> dict:
        freq = psutil.cpu_freq()
        return {
            "physical_cores": psutil.cpu_count(logical=False),
            "logical_cores": psutil.cpu_count(logical=True),
            # interval=None -> non-blocking, uses delta since last call
            # (primed in __init__, so this is a real value from request 1 onward)
            "total_percent": psutil.cpu_percent(interval=None),
            "per_core_percent": psutil.cpu_percent(interval=None, percpu=True),
            "frequency_mhz": {
                "current": round(freq.current) if freq else None,
                "min": round(freq.min) if freq else None,
                "max": round(freq.max) if freq else None,
            },
        }

    # memory
    def _get_memory(self) -> dict:
        mem = psutil.virtual_memory()
        return {
            "total_gb": round(mem.total / (1024**3), 2),
            "used_gb": round(mem.used / (1024**3), 2),
            "available_gb": round(mem.available / (1024**3), 2),
            "percent_used": mem.percent,
            "buffers_cache_gb": round(getattr(mem, "cached", 0) / (1024**3), 2),
        }

    def _get_swap(self) -> dict:
        swap = psutil.swap_memory()
        return {
            "total_gb": round(swap.total / (1024**3), 2),
            "used_gb": round(swap.used / (1024**3), 2),
            "percent_used": swap.percent,
        }

    # disks
    def _get_disks(self) -> list[dict]:
        disks = []
        for part in psutil.disk_partitions(all=False):
            try:
                usage = psutil.disk_usage(part.mountpoint)
            except (PermissionError, OSError):
                continue
            disks.append(
                {
                    "mountpoint": part.mountpoint,
                    "device": part.device,
                    "filesystem": part.fstype,
                    "total_gb": round(usage.total / (1024**3), 2),
                    "used_gb": round(usage.used / (1024**3), 2),
                    "free_gb": round(usage.free / (1024**3), 2),
                    "percent_used": usage.percent,
                }
            )
        return disks

    def _get_disk_io(self) -> dict | None:
        io = psutil.disk_io_counters()
        if io is None:
            return None
        return {
            "read_mb": round(io.read_bytes / (1024**2), 2),
            "write_mb": round(io.write_bytes / (1024**2), 2),
            "read_count": io.read_count,
            "write_count": io.write_count,
        }

    # network
    def _get_network(self) -> dict:
        io = psutil.net_io_counters()
        stats = psutil.net_if_stats()
        active_interfaces = [name for name, s in stats.items() if s.isup]
        return {
            "sent_mb": round(io.bytes_sent / (1024**2), 2),
            "received_mb": round(io.bytes_recv / (1024**2), 2),
            "packets_sent": io.packets_sent,
            "packets_received": io.packets_recv,
            "errors_in": io.errin,
            "errors_out": io.errout,
            "active_interfaces": active_interfaces,
        }

    # power / battery
    def _get_power(self) -> dict:
        batt = psutil.sensors_battery()
        if batt is None:
            return {"has_battery": False}

        seconds_left = batt.secsleft
        unknown_time = seconds_left in (
            psutil.POWER_TIME_UNLIMITED,
            psutil.POWER_TIME_UNKNOWN,
        )

        return {
            "has_battery": True,
            "percent": round(batt.percent, 1),
            "plugged_in": batt.power_plugged,
            "state": "charging" if batt.power_plugged else "discharging",
            "time_remaining_seconds": None if unknown_time else seconds_left,
            "time_remaining_human": (
                None if unknown_time else self._humanize_seconds(seconds_left)
            ),
            "status": self._battery_status_label(batt.percent, batt.power_plugged),
        }

    @staticmethod
    def _battery_status_label(percent: float, plugged_in: bool) -> str:
        if plugged_in:
            return "charging" if percent < 100 else "full"
        if percent <= 15:
            return "critical"
        if percent <= 30:
            return "low"
        return "normal"

    # temperatures
    def _get_temperatures(self) -> list[dict]:
        try:
            temps = psutil.sensors_temperatures()
        except AttributeError:
            return []

        readings = []
        for chip, entries in temps.items():
            for entry in entries:
                readings.append(
                    {
                        "chip": chip,
                        "label": entry.label or chip,
                        "current_c": entry.current,
                        "high_c": entry.high,
                        "critical_c": entry.critical,
                    }
                )
        return readings

    # processes
    def _get_top_processes(self, limit: int = 8) -> list[dict]:
        procs = []
        for p in psutil.process_iter(["pid", "name", "username", "memory_percent"]):
            try:
                # interval=None uses delta since this PID's last cpu_percent()
                # call (primed in __init__ / previous request).
                cpu = p.cpu_percent(interval=None)
                info = p.info
                procs.append(
                    {
                        "pid": info["pid"],
                        "name": info["name"],
                        "user": info["username"],
                        "cpu_percent": round(cpu, 1),
                        "memory_percent": (
                            round(info["memory_percent"], 1)
                            if info["memory_percent"]
                            else 0.0
                        ),
                    }
                )
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        procs.sort(key=lambda x: x["cpu_percent"], reverse=True)
        return procs[:limit]

    @staticmethod
    def _humanize_seconds(seconds: float) -> str:
        seconds = int(seconds)
        days, seconds = divmod(seconds, 86400)
        hours, seconds = divmod(seconds, 3600)
        minutes, _ = divmod(seconds, 60)
        parts = []
        if days:
            parts.append(f"{days}d")
        if hours:
            parts.append(f"{hours}h")
        parts.append(f"{minutes}m")
        return " ".join(parts)
