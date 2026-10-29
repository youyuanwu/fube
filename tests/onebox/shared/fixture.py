"""Partition-scoped onebox ownership, readiness, diagnostics and teardown."""

import os
from copy import copy
import re
import sys
import time

from tests.onebox.shared import ci

runtime = ci.runtime
PHASES = ci.FIXTURE_PHASES


def validate_inputs(args):
    for path in (args.report_dir, args.config_dir, args.github_output):
        if path is not None:
            ci.outside_bundle(path, args.prebuilt)
    for private in (args.config_dir, args.github_output):
        if private is not None:
            ci.require(private != args.report_dir and args.report_dir not in private.parents,
                       "private config/output must be outside uploaded evidence")
    if args.config_dir is not None:
        ci.require(not args.config_dir.exists(), "NEW private fixture config directory required")
    for filename in ci.inventory(args.partition) - {"fixture.json"}:
        ci.require(not ci.safe_path(args.report_dir / filename).exists(), "NEW partition evidence required")
    command = ["validate", "--route", args.partition, "--receipt", str(args.producer),
               "--bundle-tar", str(args.bundle_tar), "--bundle-dir", str(args.prebuilt),
               "--out", str(args.report_dir / "validated-input.json")]
    if args.archive is not None:
        command.extend(["--archive", str(args.archive)])
    ci.require(ci.main(command) == 0, "partition input validation failed")
    return ci.read(args.report_dir / "validated-input.json")


def diagnostics(args, owner):
    from scripts import linux_cluster_diagnostics

    with runtime.owned_cluster(args.name, owner) as cluster:
        cluster.resources(require_volumes=False)
        report = linux_cluster_diagnostics.collect(
            args.name, runtime.onebox_ctl.STATE_ROOT, project_prefix=cluster.project_prefix)
    with ci.reserve(args.report_dir / "diagnostics.json", args.prebuilt) as output:
        ci.write(output, report)
    ci.require(report["errors"] == [], "owned onebox diagnostics failed")


def cleanup_owned(args, owner):
    return ci.main(["cleanup", "--name", args.name, "--owner", owner, "--route", args.partition,
                    "--producer", str(args.producer), "--report", str(args.report_dir / "cleanup.json")])


def run(selected, args, execute):
    ci.require(re.fullmatch(r"[a-z][a-z0-9-]{0,39}", args.name) is not None, "allocation name")
    if args.owner is not None:
        ci.binding(args.name, args.owner)
    for suite in selected.values():
        if suite["kind"] == "nextest":
            ci.require(all(value is not None for value in (args.archive, args.config_dir, args.fixture_owner)),
                       "native-client archive, private config directory and fixture owner required")
    # Normalize before enforcing private/public path separation.
    for name in ("prebuilt", "bundle_tar", "producer", "archive", "report_dir", "config_dir", "github_output"):
        path = getattr(args, name)
        if path is not None:
            setattr(args, name, ci.safe_path(path))
    scope = copy(args)
    reused, original = scope.owner is not None, scope.owner
    result = {
        "version": 1, "kind": "onebox-fixture", "route": args.partition, "status": "FAIL", "errors": [],
        "context": None, "producer_sha256": None, "allocation": None,
        "ownership": "reused" if reused else "none", "outcomes": dict.fromkeys(PHASES, "skipped"),
        "started_at": time.time(), "ready_at": None, "finished_at": None,
    }

    def failed(phase, error):
        result["outcomes"][phase] = "failure"
        message = phase + " failed: " + type(error).__name__
        result["errors"].append(message)
        print("onebox fixture: " + message, file=sys.stderr)

    def allocated(owner):
        nonlocal original
        ci.require(not reused and original is None and ci.sha(owner, 32),
                   "immutable original creation capability")
        original, args.owner = owner, owner
        result["ownership"] = "created"
        result["allocation"] = ci.binding(scope.name, owner)
        if scope.github_output is not None:
            runtime.onebox_ctl.ctl.publish_owner(scope.github_output, owner)

    with ci.reserve(scope.report_dir / "fixture.json", scope.prebuilt) as output:
        phase = "validate"
        try:
            validated = validate_inputs(scope)
            result.update(context=validated["context"], producer_sha256=validated["producer_sha256"])
            result["outcomes"]["validate"] = "success"
            phase = "create"
            if reused:
                result["allocation"] = ci.binding(scope.name, original)
                result["outcomes"]["create"] = "reused"
            else:
                returned = runtime.create_owned(scope.name, on_allocated=allocated)
                ci.require(original is not None and returned == original, "create returned a different owner")
                result["outcomes"]["create"] = "success"
            phase = "ready"
            allocation = runtime.Allocation(scope.name, original)
            result["allocation"] = ci.binding(scope.name, original, allocation.container_id)
            result["ready_at"] = time.time()
            result["outcomes"]["ready"] = "success"
            phase = "suites"
            ci.require(execute(selected, args, ci.suites.digest()) == 0, "partition suites failed")
            result["outcomes"]["suites"] = "success"
        except (Exception, KeyboardInterrupt) as error:
            failed(phase, error)
        finally:
            if original is not None:
                if not reused or result["outcomes"]["validate"] == "success":
                    try:
                        diagnostics(scope, original)
                        result["outcomes"]["diagnostics"] = "success"
                    except (Exception, KeyboardInterrupt) as error:
                        failed("diagnostics", error)
                if reused:
                    result["outcomes"]["destroy"] = "retained"
                else:
                    try:
                        ci.require(cleanup_owned(scope, original) == 0, "original allocation cleanup failed")
                        result["outcomes"]["destroy"] = "success"
                        if scope.github_output is not None:
                            with scope.github_output.open("a", encoding="utf-8") as github:
                                github.write("cleaned=true\n")
                                github.flush()
                                os.fsync(github.fileno())
                    except (Exception, KeyboardInterrupt) as error:
                        failed("destroy", error)
            expected = {phase: "success" for phase in PHASES}
            if reused:
                expected.update(create="reused", destroy="retained")
            if not result["errors"] and result["outcomes"] == expected:
                result["status"] = "PASS"
            result["finished_at"] = time.time()
            ci.write(output, result)
    return 0 if result["status"] == "PASS" else 1
