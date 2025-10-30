#!/usr/bin/env python3

"""Python implementation of run.sh for orchestrating a local Flink workflow."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Sequence
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


JOB_ID_PATTERN = re.compile(r"JobID ([a-f0-9]{32})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Starts a local Flink cluster, submits job.py via uv, creates a savepoint "
            "to modify its state, and finally resumes the job from the modified savepoint."
        ),
        allow_abbrev=False,
    )
    parser.add_argument(
        "python_exec",
        help=(
            "Absolute path to the Python interpreter inside the desired virtual "
            "environment (e.g. from uv venv)."
        ),
    )
    parser.add_argument(
        "job_args",
        nargs=argparse.REMAINDER,
        help="Additional arguments forwarded to the Flink job (prefix with -- to stop parsing).",
    )
    return parser.parse_args()


def normalize_job_args(job_args: Sequence[str]) -> list[str]:
    if job_args and job_args[0] == "--":
        return list(job_args[1:])
    return list(job_args)


def run_command(
    cmd: Sequence[str],
    *,
    cwd: Path,
    capture_output: bool = False,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        list(cmd),
        cwd=str(cwd),
        text=True,
        stdout=subprocess.PIPE if capture_output else None,
        stderr=subprocess.STDOUT if capture_output else None,
    )
    if check and result.returncode != 0:
        if capture_output and result.stdout:
            sys.stderr.write(result.stdout)
        raise RuntimeError(
            f"Command {' '.join(cmd)} failed with exit code {result.returncode}"
        )
    return result


def extract_job_id(output: str) -> str | None:
    match = JOB_ID_PATTERN.search(output)
    return match.group(1) if match else None


def check_job_status(job_id: str, target: str) -> str:
    url = f"http://{target}/jobs/{job_id}"
    try:
        with urlopen(url, timeout=5) as response:
            payload = json.load(response)
    except (HTTPError, URLError, json.JSONDecodeError, TimeoutError, ValueError):
        return "UNKNOWN"
    return payload.get("state", "UNKNOWN")


def main() -> int:
    args = parse_args()
    job_args = normalize_job_args(args.job_args)

    python_exec = Path(args.python_exec).expanduser().resolve()
    if not python_exec.exists():
        print(f"Error: Python executable not found: {python_exec}", file=sys.stderr)
        return 1

    repo_root = Path(__file__).resolve().parent
    job_py = repo_root / "job.py"
    if not job_py.exists():
        print(
            f"Error: job.py not found at expected location: {job_py}", file=sys.stderr
        )
        return 1

    swap_tool = repo_root / "swap_sst_last5"
    if not swap_tool.exists():
        print(
            f"Error: swap tool not found at expected location: {swap_tool}",
            file=sys.stderr,
        )
        return 1

    flink_home = Path(os.environ.get("FLINK_HOME", "/opt/flink"))
    flink_bin_dir = flink_home / "bin"
    flink_cli = flink_bin_dir / "flink"
    start_cluster = flink_bin_dir / "start-cluster.sh"

    if not start_cluster.exists():
        print(f"Error: start-cluster.sh not found in {flink_bin_dir}", file=sys.stderr)
        return 1
    if not flink_cli.exists():
        print(f"Error: flink CLI not found in {flink_bin_dir}", file=sys.stderr)
        return 1

    flink_jobmanager_target = os.environ.get(
        "FLINK_JOBMANAGER_TARGET", "127.0.0.1:8081"
    )

    print("Starting Flink cluster...")
    run_command([str(start_cluster)], cwd=repo_root)

    print("Waiting for cluster to be ready...")
    time.sleep(5)

    submit_cmd = [
        "uv",
        "run",
        "--",
        str(flink_cli),
        "run",
        "-m",
        flink_jobmanager_target,
        "-d",
        "-py",
        str(job_py),
        "-pyexec",
        str(python_exec),
        *job_args,
    ]

    print("Submitting job...")
    submit_proc = run_command(
        submit_cmd, cwd=repo_root, capture_output=True, check=False
    )
    job_output = submit_proc.stdout or ""
    if job_output:
        print(job_output, end="" if job_output.endswith("\n") else "\n")
    if submit_proc.returncode != 0:
        print("Error: Job submission failed", file=sys.stderr)
        return submit_proc.returncode or 1

    job_id = extract_job_id(job_output)
    print(f"DEBUG: Extracted Job ID: '{job_id or ''}'")
    if not job_id:
        print("Error: Could not extract job ID from output", file=sys.stderr)
        return 1

    time.sleep(4)

    print("Checking job status and waiting for full initialization...")
    max_wait = 30
    wait_time = 0
    job_status = "UNKNOWN"
    while wait_time < max_wait:
        job_status = check_job_status(job_id, flink_jobmanager_target)
        print(f"Job status: {job_status} (waited {wait_time}s)")
        if job_status == "RUNNING":
            print(
                "Job is running, waiting additional 10 seconds for task initialization..."
            )
            time.sleep(10)
            break
        time.sleep(2)
        wait_time += 2

    if job_status != "RUNNING":
        print(
            f"Error: Job failed to reach RUNNING state within {max_wait} seconds. Current status: {job_status}",
            file=sys.stderr,
        )
        return 1

    print("Stopping job and creating savepoint...")
    savepoint_cmd = [
        str(flink_cli),
        "savepoint",
        job_id,
        "--type",
        "native",
    ]
    savepoint_proc = run_command(
        savepoint_cmd, cwd=repo_root, capture_output=True, check=False
    )
    savepoint_output = savepoint_proc.stdout or ""
    if savepoint_proc.returncode != 0:
        if savepoint_output:
            sys.stderr.write(savepoint_output)
        print("Error: Savepoint creation command failed", file=sys.stderr)
        return savepoint_proc.returncode or 1

    match = re.search(r"Path: file:(\S+)", savepoint_output)
    if not match:
        print("Error: Could not find savepoint path", file=sys.stderr)
        if savepoint_output:
            print("Full output was:", file=sys.stderr)
            sys.stderr.write(savepoint_output)
        return 1

    savepoint_path = Path(match.group(1))
    print(f"Savepoint created at: {savepoint_path}")

    if not savepoint_path.exists():
        print(
            f"Error: Savepoint path does not exist: {savepoint_path}", file=sys.stderr
        )
        return 1

    target_file: Path | None = None
    max_size = -1
    for candidate in savepoint_path.iterdir():
        if candidate.is_file() and candidate.name != "_metadata":
            try:
                size = candidate.stat().st_size
            except OSError:
                continue
            if size > max_size:
                max_size = size
                target_file = candidate

    if target_file is None:
        print("Error: No state file found in savepoint", file=sys.stderr)
        return 1

    print(f"Found state file: {target_file}")

    target_file_with_suffix = target_file.with_name(f"{target_file.name}.sst")
    target_file.rename(target_file_with_suffix)

    try:
        run_command([str(swap_tool), str(target_file_with_suffix)], cwd=repo_root)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        # Attempt to revert rename before exiting
        if target_file_with_suffix.exists():
            target_file_with_suffix.rename(target_file)
        return 1

    target_file_with_suffix.rename(target_file)

    print("Resuming job from modified savepoint...")
    resume_cmd = [
        "uv",
        "run",
        "--",
        str(flink_cli),
        "run",
        "-s",
        str(savepoint_path),
        "-py",
        str(job_py),
        "-pyexec",
        str(python_exec),
        *job_args,
    ]
    resume_proc = run_command(
        resume_cmd, cwd=repo_root, capture_output=True, check=False
    )
    resume_output = resume_proc.stdout or ""
    if resume_output:
        print(resume_output, end="" if resume_output.endswith("\n") else "\n")
    if resume_proc.returncode != 0:
        print("Error: Failed to resume job from savepoint", file=sys.stderr)
        return resume_proc.returncode or 1

    print(f"Flink logs location: {flink_home / 'log'}")
    print("To view logs: docker run -v $(pwd)/logs:/opt/flink/log flink-wal-test")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
