"""Collect allowlisted cluster metadata, never bootstrap XML or raw runtime logs."""

import argparse
import json
from pathlib import Path
import subprocess
import sys

from linux_cluster_ctl import STATE_ROOT, validate_name


def pick(record, fields):
    if not isinstance(record, dict):
        raise ValueError("Expected a diagnostic object")
    return {field: record[field] for field in fields if field in record}


def summarize_observation(observation):
    gateways = {}
    for name, gateway in observation.get("gateways", {}).items():
        gateways[name] = {
            "query_failed": "error" in gateway,
            "baseline": gateway.get("baseline"),
            "health": pick(gateway.get("health", {}), ("AggregatedHealthState",)),
            "upgrade": pick(gateway.get("upgrade", {}), (
                "CodeVersion", "ConfigVersion", "UpgradeState", "RollingUpgradeMode", "FailureReason",
            )),
            "nodes": [
                pick(node, (
                    "Name", "IpAddressOrFQDN", "NodeStatus", "HealthState",
                    "CodeVersion", "ConfigVersion", "IsSeedNode",
                )) for node in gateway.get("nodes", {}).get("Items", [])
            ],
        }
    return {
        "ready": observation.get("ready"),
        "problem_count": len(observation.get("problems", [])),
        "gateways": gateways,
    }


def docker(*arguments):
    return subprocess.run(
        ["docker", *arguments], text=True, capture_output=True, check=True, timeout=20,
    ).stdout


def collect(name, state_root=STATE_ROOT, *, project_prefix="sf-linux-"):
    project = validate_name(name, project_prefix)
    report = {"name": name, "project": project, "errors": []}
    directory = state_root / name
    for filename, key in (("state.json", "allocation"), ("last-status.json", "observation")):
        path = directory / filename
        if not path.exists():
            report[key] = {"available": False}
            continue
        try:
            value = json.loads(path.read_text())
            if not isinstance(value, dict):
                raise ValueError("Expected a JSON object")
            report[key] = pick(value, (
                "phase", "baseline", "image", "runtime_version", "manifest_version",
            )) if key == "allocation" else summarize_observation(value)
        except (OSError, ValueError) as error:
            report["errors"].append(f"{filename}: {type(error).__name__}")
    try:
        ids = docker("ps", "-aq", "--filter", "label=com.docker.compose.project=" + project).split()
        containers = json.loads(docker("inspect", *ids)) if ids else []
        report["containers"] = [{
            **pick(container, ("Name", "Image")),
            "state": pick(container["State"], (
                "Status", "Running", "ExitCode", "OOMKilled", "StartedAt", "FinishedAt",
            )),
        } for container in containers]
        statistics = docker("stats", "--no-stream", "--format", "{{json .}}", *ids) if ids else ""
        report["statistics"] = [
            pick(json.loads(line), ("Name", "CPUPerc", "MemUsage", "MemPerc", "PIDs"))
            for line in statistics.splitlines()
        ]
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        report["errors"].append(f"Docker diagnostics: {type(error).__name__}")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--onebox", action="store_true", help="Collect a single-container Ubuntu onebox")
    arguments = parser.parse_args()
    if arguments.onebox:
        from onebox_ctl import Onebox, STATE_ROOT as ONEBOX_STATE_ROOT

        report = collect(arguments.name, ONEBOX_STATE_ROOT, project_prefix=Onebox.project_prefix)
    else:
        report = collect(arguments.name)
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(report, indent=2) + "\n")
    for error in report["errors"]:
        print("ERROR: " + error, file=sys.stderr)
    return 1 if report["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
