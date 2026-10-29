"""Run suites/partitions selected by tests/suites.toml, or emit the CI matrix."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest

from tests import ROOT, suites


def needed(args, *names):
    for name in names:
        if getattr(args, name) is None:
            raise ValueError("--" + name.replace("_", "-") + " is required for this suite")


def command(suite, args):
    kind = suite["kind"]
    if kind == "unittest":
        return [sys.executable, "-m", "unittest", "discover", "-s", suite["start"],
                "-t", ".", "-p", suite["pattern"]]
    if kind == "nextest":
        needed(args, "archive")
        result = ["cargo-nextest", "nextest", "run", "--archive-file", str(args.archive),
                  "--workspace-remap", str(ROOT), "-E", suite["filter"], "--no-tests", "fail",
                  "--no-fail-fast"]
        if suite["ignored"]:
            result.extend(["--run-ignored", "all"])
        if suite["resource"] == "onebox":
            needed(args, "name", "report_dir", "config_dir", "fixture_owner")
        return result
    if kind in ("data", "legacy"):
        needed(args, "name", "owner", "prebuilt", "report_dir")
        report = str(args.report_dir / suite["report"])
        if kind == "data":
            return [sys.executable, "-m", "tests.onebox.shared.data", args.name,
                    "--existing-owner", args.owner, "--prebuilt", str(args.prebuilt),
                    "--profile", suite["profile"], "--report", report, "--summary"]
        needed(args, "producer")
        return [sys.executable, "-m", "tests.onebox.shared.ci", "legacy",
                "--name", args.name, "--owner", args.owner, "--prebuilt", str(args.prebuilt),
                "--producer", str(args.producer), "--report", report]
    result = [sys.executable, "-m", "tests.native_runtime.runner", suite["action"]]
    needed(args, "prebuilt")
    for name in ("prebuilt", "root", "native_directory", "dependency_directory", "diagnostics_directory"):
        value = getattr(args, name)
        if value is not None:
            result.extend(["--" + name.replace("_", "-"), str(value)])
    return result


def native_environment(suite, args):
    args.config_dir.mkdir(parents=True, exist_ok=False)
    environment = os.environ.copy()
    for prefix, report in zip(("TEST", "HEALTH", "NAME", "LIST", "NOTIFICATION"), suite["reports"]):
        destination = args.report_dir / report
        destination.parent.mkdir(parents=True, exist_ok=True)
        environment["MSSF_" + prefix + "_REPORT"] = str(destination)
        environment["MSSF_" + prefix + "_CONFIG"] = str(args.config_dir / (prefix.lower() + "-config.json"))
        if prefix in ("HEALTH", "NAME", "LIST"):
            environment["MSSF_" + prefix + "_OWNER"] = args.fixture_owner
    environment["MSSF_NOTIFICATION_ALLOCATION"] = args.name
    environment["MSSF_NOTIFICATION_JOURNAL"] = str(args.config_dir / "notification-journal.json")
    with args.archive.open("rb") as file:
        archive_hash = hashlib.file_digest(file, "sha256").hexdigest()
    environment["MSSF_TEST_ARCHIVE_SHA256"] = archive_hash
    return environment, archive_hash


def execute(selected, args, manifest_hash):
    commands = {name: command(suite, args) for name, suite in selected.items()}
    outcomes = {name: "skipped" for name in selected}
    errors = []
    output = None
    if args.report_dir is not None:
        args.report_dir.mkdir(parents=True, exist_ok=True)
        output = (args.report_dir / "suite-outcomes.json").open("x", encoding="utf-8")
    try:
        for name, suite in selected.items():
            native = suite["kind"] == "nextest" and suite["resource"] == "onebox"
            archive_hash = ""
            outcome = "failure"
            try:
                environment = None
                if native:
                    environment, archive_hash = native_environment(suite, args)
                elif suite["kind"] in ("data", "legacy"):
                    (args.report_dir / suite["report"]).parent.mkdir(parents=True, exist_ok=True)
                if suite["kind"] == "unittest":
                    loader = unittest.TestLoader()
                    discovered = loader.discover(str(ROOT / suite["start"]), suite["pattern"], str(ROOT))
                    if loader.errors:
                        raise ValueError("unittest discovery failed:\n" + "\n".join(loader.errors))
                    if not discovered.countTestCases():
                        raise ValueError("unittest selection discovered no tests")
                result = subprocess.run(commands[name], cwd=ROOT, env=environment, timeout=suite["timeout"])
                if result.returncode == 0:
                    outcome = "success"
                else:
                    errors.append(name + ": process exited with status " + str(result.returncode))
            except subprocess.TimeoutExpired:
                errors.append(name + ": configured timeout exceeded")
            except OSError as error:
                errors.append(name + ": execution failed (" + type(error).__name__ + ")")
            except ValueError as error:
                errors.append(name + ": " + str(error))
            outcomes[name] = ({"outcome": outcome, "archive_sha256": archive_hash,
                               "filter": suite["filter"], "no_tests": "fail"} if native else outcome)
            print("suite=" + name + " outcome=" + outcome, flush=True)
    finally:
        if output is not None:
            with output:
                json.dump({"version": 1, "partition": args.partition, "manifest_sha256": manifest_hash,
                           "suites": outcomes, "errors": errors,
                           "status": "PASS" if not errors and all(
                               (value["outcome"] if isinstance(value, dict) else value) == "success"
                               for value in outcomes.values()) else "FAIL"}, output)
                output.write("\n")
                output.flush()
                os.fsync(output.fileno())
    for error in errors:
        print(error, file=sys.stderr)
    return 1 if errors else 0


def main(argv=None, *, partition=None):
    parser = argparse.ArgumentParser(description=__doc__)
    if partition is None:
        selection = parser.add_mutually_exclusive_group(required=True)
        selection.add_argument("--suite", choices=suites.MANIFEST.suites)
        selection.add_argument("--partition", choices=suites.MANIFEST.partitions)
        selection.add_argument("--matrix", action="store_true")
    else:
        parser.set_defaults(partition=partition, suite=None, matrix=False)
    for name in ("name", "fixture-owner"):
        parser.add_argument("--" + name)
    parser.add_argument("--existing-owner", "--owner", dest="owner",
                        help="reuse an exact caller-owned allocation; never destroy it")
    for name in ("prebuilt", "producer", "archive", "report-dir", "config-dir", "root",
                 "native-directory", "dependency-directory", "diagnostics-directory", "bundle-tar", "github-output"):
        parser.add_argument("--" + name, type=Path)
    args = parser.parse_args(argv)
    if args.matrix:
        print(json.dumps(suites.MANIFEST.matrix(), separators=(",", ":")))
        return 0
    try:
        if args.partition is not None:
            needed(args, "name", "prebuilt", "producer", "bundle_tar", "report_dir")
            selected = suites.MANIFEST.selected(args.partition)
            from tests.onebox.shared import fixture
            return fixture.run(selected, args, execute)
        else:
            selected = {args.suite: suites.MANIFEST.suites[args.suite]}
        return execute(selected, args, suites.digest())
    except (OSError, ValueError, RuntimeError) as error:
        print("suite runner failed: " + str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
