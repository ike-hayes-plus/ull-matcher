#!/usr/bin/env python3
import os
import platform
import subprocess


BENCHMARK_SCHEMA_VERSION = 1


def benchmark_metadata():
    return {
        "benchmarkSchemaVersion": BENCHMARK_SCHEMA_VERSION,
        "pythonVersion": platform.python_version(),
        "osName": platform.system() or "unknown",
        "osRelease": platform.release() or "unknown",
        "osArch": platform.machine() or "unknown",
        "availableProcessors": os.cpu_count() or 0,
        "gitCommit": git_commit(),
    }


def git_commit():
    value = os.environ.get("ULL_MATCHER_GIT_COMMIT") or os.environ.get("GIT_COMMIT")
    if value:
        return value
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=1.0,
        )
        return completed.stdout.strip() or "unknown"
    except (OSError, subprocess.SubprocessError):
        return "unknown"
