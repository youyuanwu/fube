#!/usr/bin/env python3
"""CI artifact receipts and compiler-free whole-suite onebox routes.

This adapter does not create allocations or build inputs. Cleanup always uses
the caller's original capability, including when input validation has failed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
from itertools import combinations
import re
import stat
import subprocess
import sys
import tarfile
import time

from tests.onebox.shared import data
from tests import suites

runtime = data.runtime
ROOT = data.ROOT
SMALL = 1024 * 1024
REPORT = 512 * SMALL
INPUT = 4 * 1024 * SMALL
ROUTES = tuple(suites.MANIFEST.partitions)
ONEBOX_SUITES = {name: suite for name, suite in suites.MANIFEST.suites.items()
                if suite["resource"] == "onebox"}
OUTCOMES = {"validate", "create", "destroy"} | ONEBOX_SUITES.keys()
FIXTURE_PHASES = ("validate", "create", "ready", "suites", "diagnostics", "destroy")
REQUIRED_JOBS = ("producer", "workers", "rust_tests", "rust_quality", "python_tests", "standalone")
NATIVE_FILES = tuple(Path(report).name for suite in ONEBOX_SUITES.values()
                     if suite["kind"] == "nextest" for report in suite["reports"])
CLEAR_CASES = (
    "native_ping", "native_name_exists_existing", "native_name_exists_absent",
    "native_concurrent_mixed", "clone_drop_preserves_original_connection",
    "explicit_close", "test_owned_proxy_idle_disconnect",
)
HEALTH_CASES = (
    "healthy", "inherited", "false-omitted", "true-omitted", "default", "all", "none",
    "ok", "warning", "error-false", "combined", "error-true", "warning-true",
    "none-true", "none-or-error", "raw-error", "raw-only",
)
NOTIFICATION_CASES = (
    "native session establishment", "three independent notification clients",
    "exact/prefix/primary-only registration", "singleton, named and signed-int64 resolution",
    "previous-result refresh", "independent replica endpoint/role multiset comparison",
    "native endpoint count change and empty invalidations",
    "owned primary replacement and versioned updates",
    "forced successful ACK loss, gateway retry, no duplicate delivery",
    "native error and healthy reuse", "explicit unregister", "independent close",
)
LEGACY_STATUS = "PASS_FULL_EMPTY_STATEFUL_CONTROL_PLANE_AND_STATELESS_PROFILE"


class ProofError(RuntimeError):
    pass


def require(value, message):
    if not value:
        raise ProofError(message)


def exact(value, keys, message):
    require(type(value) is dict and set(value) == set(keys), message)


def number(value, minimum=0):
    return type(value) in (int, float) and math.isfinite(value) and value >= minimum


def integer(value, minimum=0):
    return type(value) is int and value >= minimum


def text(value):
    return type(value) is str and 0 < len(value) <= 1024


def sha(value, length=64):
    return type(value) is str and re.fullmatch(r"[a-f0-9]{" + str(length) + "}", value)


def owner_hash(owner):
    require(sha(owner, 32), "original owner capability required")
    return hashlib.sha256(owner.encode("ascii")).hexdigest()


def safe_path(path):
    path = Path(os.path.abspath(path))
    require(not any(part.is_symlink() for part in (path, *path.parents)), "symlink path rejected")
    return path


def regular(path, limit):
    path = safe_path(path)
    info = path.stat()
    require(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= limit, "regular file byte bound")
    return path


def snapshot(path):
    info = path.stat()
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def identity(path, limit=INPUT):
    path = regular(path, limit)
    before = snapshot(path)
    digest = runtime.digest(path)
    require(snapshot(path) == before, "file changed while hashing")
    return {"sha256": digest, "bytes": before[2]}


def outside_bundle(path, bundle=None):
    path = safe_path(path)
    if bundle is not None:
        root = safe_path(bundle).resolve()
        require(path != root and root not in path.parents, "receipt must be outside strict bundle")
    require(not any((parent / "build-info.json").exists() for parent in path.parents),
            "receipt must be outside bundle metadata directories")
    return path


def reserve(path, bundle=None):
    path = outside_bundle(path, bundle)
    path.parent.mkdir(parents=True, exist_ok=True)
    safe_path(path.parent)
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY
                         | getattr(os, "O_NOFOLLOW", 0), 0o644)
    return os.fdopen(descriptor, "w", encoding="utf-8")


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def same(left, right):
    return encode(left) == encode(right)


def write(file, value, limit=SMALL):
    encoded = encode(value)
    require(len(encoded.encode("utf-8")) + 1 <= limit, "output byte bound")
    file.write(encoded + "\n")
    file.flush()
    os.fsync(file.fileno())


def small_json(value):
    require(type(value) is str and len(value.encode("utf-8")) <= SMALL, "JSON argument byte bound")
    return data.strict_json(value)


class PrefixSpec:
    """Select existing workload identities without retaining the whole ledger."""

    def __init__(self, prefix, spec):
        self.prefix, self.spec = prefix, spec


class DigestSpec:
    """Hash a bounded selected map locally, never trust a producer-supplied digest."""

    def __init__(self, spec):
        self.spec = spec


def content_digest(value):
    return {"count": len(value), "sha256": hashlib.sha256(encode(value).encode("utf-8")).hexdigest()}


class Projection:
    """Streaming strict JSON projection; even discarded objects reject duplicates.

    Only selected evidence is retained. Large ledgers, samples and diagnostics
    are read, syntax checked and discarded, never copied into worker proofs.
    """

    def __init__(self, file, limit=REPORT):
        self.file, self.buffer, self.pos, self.eof = file, "", 0, False
        self.retained, self.consumed, self.limit = 0, 0, limit
        self.decoder = json.JSONDecoder()

    def more(self):
        chunk = self.file.read(65536)
        self.consumed += len(chunk.encode("utf-8"))
        require(self.consumed <= self.limit, "streamed JSON byte bound")
        self.buffer = self.buffer[self.pos:] + chunk
        self.pos = 0
        if not self.buffer:
            self.eof = True

    def peek(self):
        while True:
            if self.pos >= len(self.buffer):
                if self.eof:
                    return ""
                self.more()
                continue
            char = self.buffer[self.pos]
            if char in " \t\r\n":
                self.pos += 1
                continue
            return char

    def take(self, expected):
        require(self.peek() == expected, "malformed JSON structure")
        self.pos += 1

    def scalar(self):
        self.peek()
        while True:
            try:
                value, end = self.decoder.raw_decode(self.buffer, self.pos)
                require(type(value) not in (dict, list), "invalid JSON scalar")
                incomplete_number = (type(value) in (int, float) and end < len(self.buffer)
                                     and self.buffer[end] in ".eE+-")
                if (end == len(self.buffer) or incomplete_number) and not self.eof:
                    require(len(self.buffer) - self.pos <= 4 * SMALL, "JSON scalar byte bound")
                    old = len(self.buffer) - self.pos
                    self.more()
                    if len(self.buffer) == old:
                        self.eof = True
                    continue
                require(end == len(self.buffer) or self.buffer[end] in " \t\r\n,]}:",
                        "malformed JSON scalar delimiter")
                require(value is None or type(value) in (str, bool, int)
                        or (type(value) is float and math.isfinite(value)), "nonfinite JSON value")
                self.pos = end
                return value
            except json.JSONDecodeError:
                require(not self.eof and len(self.buffer) - self.pos <= 4 * SMALL,
                        "malformed or oversized JSON scalar")
                old = len(self.buffer) - self.pos
                self.more()
                if len(self.buffer) == old:
                    self.eof = True

    def value(self, spec, depth=0):
        if isinstance(spec, DigestSpec):
            retained = self.retained
            selected = self.value(spec.spec, depth)
            result = content_digest(selected)
            # The selected map is independently bounded during parsing, then
            # released. Account only its locally computed canonical digest.
            self.retained = retained + len(encode(result)) + 8
            require(self.retained <= SMALL, "selected evidence byte bound")
            return result
        require(depth <= 96, "JSON nesting bound")
        char = self.peek()
        require(char, "truncated JSON")
        require(not isinstance(spec, PrefixSpec) or char == "{", "workload identity map required")
        if char == "{":
            self.pos += 1
            seen, result = set(), {}
            if self.peek() != "}":
                while True:
                    require(self.peek() == '"', "JSON object key required")
                    key = self.scalar()
                    require(type(key) is str and len(key) <= 4096 and key not in seen,
                            "duplicate or oversized JSON key")
                    seen.add(key)
                    require(len(seen) <= 16384, "JSON object inventory bound")
                    self.take(":")
                    if isinstance(spec, PrefixSpec):
                        child = spec.spec if key.startswith(spec.prefix) else None
                    else:
                        child = True if spec is True else spec.get(key, spec.get("*")) if spec else None
                    value = self.value(child, depth + 1)
                    if child is not None:
                        result[key] = value
                        self.retained += len(key) + 8
                    if self.peek() == "}":
                        break
                    self.take(",")
            self.take("}")
            self.retained += 2 if spec is not None else 0
            require(self.retained <= SMALL, "selected evidence byte bound")
            return result if spec is not None else None
        if char == "[":
            self.pos += 1
            result, count = [], 0
            child = True if spec is True else spec.get("*") if spec else None
            if self.peek() != "]":
                while True:
                    value = self.value(child, depth + 1)
                    count += 1
                    require(count <= 1000000, "JSON array inventory bound")
                    if child is not None:
                        result.append(value)
                        self.retained += 8
                    if self.peek() == "]":
                        break
                    self.take(",")
            self.take("]")
            require(self.retained <= SMALL, "selected evidence byte bound")
            return result if spec is not None else None
        value = self.scalar()
        if spec is not None:
            self.retained += len(encode(value)) + 8
            require(self.retained <= SMALL, "selected evidence byte bound")
            return value
        return None


def read(path, limit=SMALL, spec=True):
    path = regular(path, limit)
    before = snapshot(path)
    with path.open(encoding="utf-8") as file:
        parser = Projection(file, limit)
        result = parser.value(spec)
        require(not parser.peek(), "trailing JSON data")
    require(snapshot(path) == before, "report changed while reading")
    require(type(result) is dict, "JSON object required")
    return result


def git(*args):
    result = subprocess.run(["git", *args], cwd=ROOT, text=True, capture_output=True,
                            timeout=30, check=True)
    require(len(result.stdout) <= SMALL, "git evidence byte bound")
    return result.stdout.strip()


def context():
    require(not git("status", "--porcelain", "--untracked-files=normal"), "checkout must be clean")
    source, tree = git("rev-parse", "HEAD"), git("rev-parse", "HEAD^{tree}")
    checkout = os.environ.get("GITHUB_SHA")
    head = os.environ.get("FUBE_CI_HEAD") or checkout
    merge = os.environ.get("FUBE_CI_MERGE") or None
    run, attempt = os.environ.get("GITHUB_RUN_ID"), os.environ.get("GITHUB_RUN_ATTEMPT")
    require(all(sha(value, 40) for value in (source, tree, checkout, head))
            and source == checkout, "actual checkout/source SHA mismatch")
    require(merge is None or (sha(merge, 40) and merge == checkout), "merge must be actual checkout")
    require(type(run) is str and re.fullmatch(r"[1-9][0-9]{0,19}", run)
            and type(attempt) is str and re.fullmatch(r"[1-9][0-9]{0,9}", attempt),
            "positive current run and attempt required")
    if os.environ.get("FUBE_CI_RUN_ID"):
        require(os.environ["FUBE_CI_RUN_ID"] == run, "CI run labels disagree")
    return {"source": source, "tree": tree, "checkout": checkout, "head": head,
            "merge": merge, "run_id": run, "attempt": attempt, "clean": True}


def tar_matches(path, directory):
    expected = {name: identity(directory / name) for name in (*data.expected_files(), "build-info.json")}
    directories = {parent.as_posix() for name in expected for parent in Path(name).parents
                   if parent != Path(".")}
    seen, seen_dirs, total = set(), set(), 0
    with tarfile.open(regular(path, INPUT), mode="r|*") as archive:
        for index, member in enumerate(archive):
            require(index < 160, "tar inventory bound")
            name = member.name
            while name.startswith("./"):
                name = name[2:]
            if name in ("", "."):
                require(member.isdir(), "tar root must be a directory")
                continue
            require(not name.startswith("/") and ".." not in name.split("/"), "tar path rejected")
            name = name.rstrip("/") if member.isdir() else name
            if member.isdir():
                require(name in directories and name not in seen_dirs, "tar directory inventory")
                seen_dirs.add(name)
                continue
            require(member.isfile() and name in expected and name not in seen, "tar file inventory")
            seen.add(name)
            item = expected[name]
            require(member.size == item["bytes"] and member.mode == stat.S_IMODE((directory / name).stat().st_mode),
                    "tar member size/mode mismatch")
            total += member.size
            require(total <= INPUT, "tar expanded byte bound")
            with archive.extractfile(member) as file:
                require(hashlib.file_digest(file, "sha256").hexdigest() == item["sha256"],
                        "tar member content mismatch")
    require(seen == set(expected), "tar incomplete bundle")


def bundle_identity(tar, directory, archive=None):
    bundle = data.load_bundle(safe_path(directory))
    tar_matches(tar, bundle.directory)
    inputs = {
        "bundle_tar": identity(tar), "manifest": identity(bundle.directory / "build-info.json"),
        "nested_manifest": identity(bundle.directory / "legacy/build-info.json"),
        "app": identity(bundle.directory / "bin/rust-runtime-app"),
        "observer": identity(bundle.directory / "bin/runtime_observer"),
        "native_oracle": identity(bundle.directory / "bin/native-replicator-oracle"),
        "qualification_script": identity(ROOT / "tests/onebox/shared/data.py"),
        "runtime_script": identity(ROOT / "tests/onebox/shared/runtime.py"),
        "suite_manifest": identity(suites.PATH),
    }
    if archive is not None:
        inputs["native_archive"] = identity(archive)
    return bundle, inputs


def receipt(path, kind, current):
    value = read(path)
    require(type(value.get("version")) is int and value["version"] == 1
            and value.get("kind") == kind and value.get("status") == "PASS"
            and value.get("errors") == [] and same(value.get("context"), current),
            "receipt kind/status/current context mismatch")
    return value


def attempt_context(current, attempt):
    require(type(attempt) is str and re.fullmatch(r"[1-9][0-9]{0,9}", attempt)
            and int(attempt) <= int(current["attempt"]), "invalid or future execution attempt")
    return {**current, "attempt": attempt}


def producer(path, current):
    selected = attempt_context(
        current, os.environ.get("FUBE_CI_PRODUCER_ATTEMPT", current["attempt"]))
    value = receipt(path, "producer", selected)
    exact(value, {"version", "kind", "status", "errors", "context", "inputs", "build", "created_at"},
          "producer receipt schema")
    exact(value["inputs"], {"bundle_tar", "manifest", "nested_manifest", "app", "observer",
                           "native_oracle", "native_archive", "qualification_script", "runtime_script",
                           "suite_manifest", "standalone_bundle"},
          "producer input inventory")
    for item in value["inputs"].values():
        exact(item, {"sha256", "bytes"}, "input identity schema")
        require(sha(item["sha256"]) and integer(item["bytes"], 1) and item["bytes"] <= INPUT,
                "input identity hash/size")
    require(same(value["inputs"]["suite_manifest"], identity(suites.PATH)), "suite manifest changed")
    build = value["build"]
    exact(build, {"target", "profile", "rustc", "cargo", "nextest", "cargo_lock", "toolchain"},
          "Rust build metadata")
    require(build["target"] == "x86_64-unknown-linux-gnu" and build["profile"] == "debug"
            and all(text(build[key]) for key in ("rustc", "cargo", "nextest"))
            and same(build["cargo_lock"], identity(ROOT / "Cargo.lock"))
            and same(build["toolchain"], identity(ROOT / "rust-toolchain.toml")),
            "Rust build target/toolchain/lockfile mismatch")
    require(number(value["created_at"], 1), "producer timestamp")
    return value


def build_metadata():
    return {
        "target": "x86_64-unknown-linux-gnu", "profile": "debug",
        "rustc": runtime.run("rustc", "--version"),
        "cargo": runtime.run("cargo", "--version"),
        "nextest": runtime.run("cargo-nextest", "nextest", "--version"),
        "cargo_lock": identity(ROOT / "Cargo.lock"), "toolchain": identity(ROOT / "rust-toolchain.toml"),
    }


def produce(args):
    current = context()
    bundle, inputs = bundle_identity(args.bundle_tar, args.bundle_dir, args.archive)
    inputs["standalone_bundle"] = identity(args.standalone_bundle)
    build = build_metadata()
    require(bundle.source_revision == current["source"], "bundle is not actual checkout source")
    require(same(context(), current), "source changed during input verification")
    return {"context": current, "inputs": inputs, "build": build, "created_at": time.time()}


def verify_artifact(args):
    current = context()
    built = producer(args.producer, current)
    require(same(identity(args.file), built["inputs"][args.input]), "producer/artifact mismatch")
    require(same(context(), current), "source changed during artifact verification")
    return {"context": current, "input": args.input, "identity": built["inputs"][args.input]}


def validate(args):
    current = context()
    built = producer(args.receipt, current)
    require(not any(suite["kind"] == "nextest" for suite in suites.MANIFEST.selected(args.route).values())
            or args.archive is not None, "native-client suite requires archive")
    bundle, inputs = bundle_identity(args.bundle_tar, args.bundle_dir, args.archive)
    require(bundle.source_revision == current["source"], "bundle source mismatch")
    require(all(same(built["inputs"][key], value) for key, value in inputs.items()), "producer/input mismatch")
    require(same(context(), current), "source changed during input verification")
    return {"route": args.route, "context": current, "inputs": built["inputs"],
            "producer_sha256": identity(args.receipt, SMALL)["sha256"], "validated_at": time.time()}


def binding(name, owner, container=None):
    require(type(name) is str and re.fullmatch(r"[a-z][a-z0-9-]{0,39}", name), "allocation name")
    result = {"name": name, "owner_sha256": owner_hash(owner)}
    if container is not None:
        require(text(container), "original container identity required")
        result["container_id"] = container
    return result


def report_binding(report):
    return binding(report["onebox"], report["owner"], report["container_id"])


def legacy(args):
    current = context()
    built = producer(args.producer, current)
    bundle = data.load_bundle(safe_path(args.prebuilt))
    require(bundle.source_revision == current["source"]
            and bundle.manifest_sha256 == built["inputs"]["manifest"]["sha256"]
            and same(identity(bundle.directory / "legacy/build-info.json"), built["inputs"]["nested_manifest"]),
            "legacy outer/nested producer binding")
    original = binding(args.name, args.owner)
    require(same(context(), current), "source changed before legacy execution")
    allocation = runtime.Allocation(args.name, args.owner)
    result = {"version": 1, "kind": "legacy", "profile": "legacy", "status": "FAIL", "errors": [],
              "scenarios": {"legacy_full": {"status": "MISSING"}}}
    data.legacy_full(allocation, bundle, result)
    result.update(context=current, producer_sha256=identity(args.producer, SMALL)["sha256"],
                  allocation={**original, "container_id": allocation.container_id},
                  provenance=data.provenance(bundle))
    if result["errors"] == [] and result["scenarios"]["legacy_full"].get("status") == "PASS":
        try:
            validate_legacy(result, built)
            result["status"] = "PASS"
        except Exception:
            result["errors"].append("actual hardened legacy policy proof validation failed")
    return result


def cleanup(args):
    errors, built, current = [], None, None
    original = binding(args.name, args.owner)
    started = time.time()
    try:
        current = context()
        built = producer(args.producer, current)
        producer_digest = identity(args.producer, SMALL)["sha256"]
    except Exception:
        errors.append("producer/context validation failed")
        producer_digest = None
    try:
        destroyed = runtime.destroy_owned(args.name, args.owner)
    except (Exception, KeyboardInterrupt) as error:
        errors.append("original-capability destroy failed: " + type(error).__name__)
        destroyed = None
    result = {"context": current, "producer_sha256": producer_digest, "route": args.route,
              "allocation": original, "destroy": destroyed, "started_at": started,
              "finished_at": time.time(), "errors": errors}
    if destroyed is not None:
        try:
            validate_destroy(result, successful=False)
        except Exception:
            errors.append("independent original absence proof invalid")
    return result


def true_fields(value, fields):
    require(type(value) is dict and all(value.get(key) is True for key in fields),
            "essential success/negative/cleanup field missing or false")


def nonempty(value):
    return type(value) in (dict, list) and bool(value)


def interval(value, start="started_at", end="finished_at"):
    require(number(value.get(start), 1) and number(value.get(end), 1)
            and value[start] <= value[end], "actual evidence timing endpoints")
    return [value[start], value[end]]


def absence(upload):
    require(type(upload) is dict and upload.get("absence_verified") is True
            and same(upload.get("native_response"), {"status": 404, "body": None})
            and text(upload.get("path")), "independent native ImageStore absence")


def validate_destroy(value, successful=True):
    destroy = value.get("destroy")
    exact(destroy, {"name", "phase", "data_deleted", "independent_absence", "absence_verified"},
          "original destroy receipt schema")
    require(destroy["name"] == value["allocation"]["name"] and destroy["phase"] == "destroyed",
            "original allocation destruction identity/phase")
    true_fields(destroy, ("data_deleted", "absence_verified"))
    inventories = destroy["independent_absence"]
    exact(inventories, {"container", "volume", "network"}, "original absence inventory")
    for kind, entries in inventories.items():
        require(type(entries) is list and all(text(entry) for entry in entries)
                and len(entries) == len(set(entries)) and len(entries) <= 4,
                "original resource identity inventory")
        if successful:
            require(len(entries) == 1, "successful worker requires one original resource per kind")
    if successful:
        require(inventories["volume"] == ["sf-onebox-" + value["allocation"]["name"] + "_data"],
                "original volume/allocation binding")
    interval(value)
    return inventories


def source_provenance(value, built):
    current, inputs = built["context"], built["inputs"]
    require(value.get("executable_source_revision") == current["source"]
            and value.get("bundle_manifest_sha256") == inputs["manifest"]["sha256"]
            and value.get("ci_run_id") == current["run_id"]
            and value.get("ci_head") == (os.environ.get("FUBE_CI_HEAD") or None)
            and value.get("ci_merge") == current["merge"]
            and value.get("qualification_script_sha256") == inputs["qualification_script"]["sha256"],
            "suite executable/source/CI provenance binding")


def validate_legacy(value, built):
    require(type(value.get("version")) is int and value["version"] == 1
            and value.get("kind") == "legacy" and value.get("profile") == "legacy"
            and value.get("errors") == [], "legacy receipt/profile/errors")
    exact(value["scenarios"], {"legacy_full"}, "legacy exact scenario inventory")
    scenario = value["scenarios"]["legacy_full"]
    require(scenario.get("status") == "PASS", "legacy scenario failed")
    interval(scenario)
    qualified = scenario["qualification"]
    require(qualified.get("status") == LEGACY_STATUS and qualified.get("errors") == [],
            "legacy exact nested status/errors")
    require(qualified.get("sf_version") == "11.8.121.4", "legacy native runtime profile")
    for field in ("stateful_singleton", "stateful_three", "resilience", "restart",
                  "stateful_after_legacy_recovery", "configuration_upgrade"):
        require(type(qualified.get(field)) is dict, "legacy complete dispatch evidence")
    inputs = built["inputs"]
    require(qualified.get("executable_source_revision") == built["context"]["source"]
            and qualified.get("bundle_manifest_sha256") == inputs["nested_manifest"]["sha256"]
            and qualified.get("binary_sha256") == {
                "rust-runtime-app": inputs["app"]["sha256"], "runtime_observer": inputs["observer"]["sha256"]},
            "legacy nested source/executable binding")
    clean = qualified["cleanup"]
    true_fields(clean, ("safe_owner_cleanup",))
    require(not clean.get("retained") and not clean.get("retained_for_recovery")
            and same(clean.get("independent_absence"), {
                "application": True, "types": {"1.1": True, "1.2": True},
                "uploads": {"1.1": True, "1.2": True}}), "legacy independent both-version absence")
    exact(qualified["deployment_versions"], {"1.1", "1.2"}, "legacy real policy versions")
    exact(clean["image_store_uploads"], {"1.1", "1.2"}, "legacy native upload absence versions")
    for version, record in qualified["deployment_versions"].items():
        generation = record.get("original_type", {}).get("native_generation")
        require(record.get("outcomes", {}).get("provision") == "accepted"
                and type(generation) in (tuple, list) and len(generation) == 3
                and all(integer(part) for part in generation)
                and record.get("original_type", {}).get("version") == version
                and nonempty(record.get("upload")), "legacy original policy execution/generation")
        absence(clean["image_store_uploads"][version])
    require(qualified.get("deployment_outcomes", {}).get("create") == "accepted"
            and text(qualified.get("original_application_identity", {}).get("native_application_id")),
            "legacy original application creation proof")
    require(same(value["allocation"], report_binding(qualified)), "legacy original allocation binding")
    source_provenance(value["provenance"], built)
    return {"allocation": value["allocation"], "timing": interval(scenario),
            "assertions": ["legacy/legacy_full"]}


def validate_copy(value, ack=False, source_kind=None):
    require(value.get("live_copy", {}).get("commits_inside_observed_build_horizon_window", 0) >= 100
            and integer(value["live_copy"]["commits_inside_observed_build_horizon_window"], 100)
            and integer(value.get("concurrent_writes", {}).get("successful"), 100),
            "native copy must overlap at least 100 actual writes")
    if ack:
        gate = value["destination_copy_ack"]
        true_fields(gate, ("blocked_after_receive", "released"))
        require(integer(gate.get("negative_observations"), 2),
                "native copy independent receive-before-ACK negative gate")
        held = gate["held_operation"]
        require(integer(held.get("sequence"), 1) and integer(held.get("boundary"), 1),
                "held native copy operation/boundary")
        replica = gate["completed_native_replica"]
        require(integer(replica.get("replica"), 1) and replica.get("status") == "Ready",
                "same completed native copy replica")
        hold = replica["rpc"]["copy_ack_hold"]
        true_fields(hold, ("released", "acknowledged", "copy_end_received"))
        require(all(type(hold.get(field)) is int and hold[field] == held[field]
                   for field in ("sequence", "boundary")), "held/completed copy identity")
        require(all(hold.get(field) is False for field in ("held", "expired", "cancelled")),
                "native copy hold did not complete safely")
        eos = gate["source_eos"]
        if source_kind == "rust":
            require(eos.get("kind") == "destination_EOS_receipt_at_source",
                   "Rust source requires destination copy EOS receipt")
            peer = eos["peer"]
            require(integer(peer.get("replica"), 1) and peer["replica"] == replica["replica"]
                   and type(peer.get("copy_horizon")) is int and peer["copy_horizon"] == held["boundary"]
                   and type(peer.get("copy_received")) is int and peer["copy_received"] == -1
                   and integer(peer.get("copy_applied")) and peer["copy_applied"] < held["sequence"]
                   and integer(peer.get("copy_finish"), held["boundary"] + 1)
                   and integer(peer.get("applied")) and peer["applied"] < peer["copy_finish"]
                   and peer.get("building") is True,
                   "destination EOS receipt must precede actual held ACK/build finish")
        else:
            require(source_kind == "native-classic-v1"
                   and eos.get("kind") == "native_source_enumeration_EOS_and_destination_CopyEnd_receipt",
                   "native source requires real enumeration EOS evidence")
            observation = eos["observation"]
            require(type(observation.get("requested_horizon")) is int
                   and observation["requested_horizon"] == held["boundary"]
                   and observation.get("enumeration_end") is True
                   and all(integer(observation.get(field), 1)
                           for field in ("items", "begin_count", "end_count", "callback_count"))
                   and observation["begin_count"] >= observation["items"] + 1
                   and observation["items"] <= observation["end_count"] <= observation["begin_count"]
                   and observation["items"] <= observation["callback_count"] <= observation["begin_count"],
                   "actual native enumeration EOS must bind the held copy boundary")


def epoch(value):
    item = value["native_partition"]["epoch"]
    exact(item, {"data_loss", "configuration"}, "actual native epoch schema")
    require(all(integer(part) for part in item.values()), "native epoch integer fields")
    return item["data_loss"], item["configuration"]


def replica_identity(rpc):
    require(all(integer(rpc.get(key), 1) for key in ("replica", "pid"))
            and (integer(rpc.get("incarnation"), 1) if rpc.get("engine_kind") != "native-classic-v1"
                 else text(rpc.get("incarnation")) and rpc["incarnation"] == rpc.get("engine_incarnation"))
            and all(text(rpc.get(key)) for key in ("partition", "node", "engine_incarnation", "host")),
            "actual data replica/process identity required")
    return tuple(rpc[key] for key in
                 ("partition", "node", "replica", "incarnation", "engine_incarnation", "host", "pid"))


def data_snapshot(value, profile, service=None, target=3, minimum=2):
    require(value.get("observer") == "fube-client" and text(value.get("service"))
            and text(value.get("partition")) and (service is None or value["service"] == service),
            "independent native data observation identity")
    native = value["native_partition"]
    require(type(native.get("target")) is int and native["target"] == target
            and type(native.get("minimum")) is int and native["minimum"] == minimum
            and native.get("status") == "Ready", "actual native data configuration size")
    epoch(value)
    replicas = value["replicas"]
    require(type(replicas) is list and len(replicas) == target, "actual ready data replica count")
    nodes, identities, primaries = set(), set(), []
    for replica in replicas:
        rpc = replica["rpc"]
        identity = replica_identity(rpc)
        kind = "rust" if profile == "rust" else data.NODE_ENGINES.get(replica.get("node"))
        require(kind in ("rust", "native-classic-v1") and rpc.get("engine_kind") == kind
                and (profile == "rust" or replica["node"] in data.MIXED_NODES)
                and rpc.get("profile") == "fube-data-v1"
                and replica.get("status") == "Ready"
                and replica.get("role") in ("Primary", "ActiveSecondary")
                and same(replica.get("replica"), rpc["replica"])
                and replica.get("node") == rpc["node"] and rpc["partition"] == value["partition"]
                and same(replica["deployed"].get("host_process_id"), rpc["pid"])
                and text(replica.get("address")) and replica["address"] == rpc.get("service_address"),
                "native placement/deployed PID/HTTP data identity binding")
        role = replica["role"]
        require(rpc.get("role") == rpc.get("current_role") == role
                and type(replica.get("service_http_status")) is int
                and replica["service_http_status"] == (200 if role == "Primary" else 503),
                "actual native role and authoritative route")
        state, engine = rpc["state"], rpc["engine"]
        require(integer(state.get("last")) and state.get("copy_in_progress") is False
                and engine.get("closed") is False and engine.get("fault") is None
                and engine.get("copy_done") is True
                and all(integer(engine["progress"].get(key)) for key in PROGRESS_SPEC),
                "actual data state/progress readiness")
        require(replica["node"] not in nodes and identity not in identities,
                "independent data replica identities")
        nodes.add(replica["node"])
        identities.add(identity)
        if replica["role"] == "Primary":
            primaries.append(replica)
    require(len(primaries) == 1, "one actual native primary")
    primary, placed = value["primary"], primaries[0]
    # The resolver's HTTP observation is taken after the placed-replica poll.
    # ACK/completion progress may advance between them without a new mutation.
    require(primary.get("resolver") == "fube-client" and primary.get("endpoint") == placed["address"]
            and same(primary.get("native_pid"), placed["rpc"]["pid"])
            and replica_identity(primary["rpc"]) == replica_identity(placed["rpc"])
            and all(same(primary["rpc"].get(key), placed["rpc"][key])
                    for key in placed["rpc"] if key not in ("state", "engine"))
            and all(same(primary["rpc"]["engine"].get(key), item)
                    for key, item in placed["rpc"]["engine"].items() if key != "progress")
            and all(integer(primary["rpc"]["engine"]["progress"].get(key))
                    and primary["rpc"]["engine"]["progress"][key] >= placed["rpc"]["engine"]["progress"][key]
                    for key in PROGRESS_SPEC)
            and all(same(primary["rpc"]["state"].get(key), item)
                    for key, item in placed["rpc"]["state"].items() if key not in ("results", "values")),
            "native resolver must bind the actual primary")
    return placed["rpc"]


def successful_writes(value, minimum):
    count, sequences, ids = value.get("successful"), value.get("sequences"), value.get("ids")
    require(integer(count, minimum) and type(value.get("requested")) is int
            and value["requested"] == count and type(sequences) is list and type(ids) is list
            and len(sequences) == len(ids) == count
            and all(integer(sequence, 1) for sequence in sequences)
            and sequences == sorted(set(sequences))
            and all(text(request_id) for request_id in ids) and len(set(ids)) == count,
            "successful writes need nonzero unique ordered sequences and identities")
    return dict(zip(ids, sequences))


def duplicate_result(reply, results):
    require(reply.get("duplicate") is True and reply.get("outcome") in ("committed", "ambiguous")
            and text(reply.get("request_id")) and reply["request_id"] in results
            and integer(reply.get("sequence"), 1)
            and same(reply["sequence"], results[reply["request_id"]]),
            "identified duplicate must retain its original accepted sequence")


def rejected_record(record, capacity=False):
    require(type(record) is dict and text(record.get("id")) and record.get("sequence") is None
            and "sequence" in record and record.get("committed") is False,
            "rejection cannot manufacture an accepted sequence/commit")
    attempts = record["attempts"]
    require(type(attempts) is list and attempts, "actual rejection attempt required")
    for attempt in attempts:
        reply = attempt["reply"]
        require(type(attempt.get("http_status")) is int and attempt["http_status"] == 503
                and reply.get("outcome") == "rejected" and reply.get("sequence") is None
                and reply.get("request_id") == record["id"], "actual pre-admission rejection required")
        if capacity:
            error = reply.get("error")
            require(text(error) and "provider" in error.lower()
                    and any(token in error.lower() for token in ("identity ledger", "history", "resource")),
                    "actual provider capacity rejection required")


def validate_process(captured, rpc, built, action):
    environment = captured["activation_environment"]
    kind = rpc["engine_kind"]
    require(captured.get("action") == action and same(captured.get("pid"), rpc["pid"])
            and captured.get("host") == rpc["host"]
            and integer(captured.get("start_ticks"), 1) and integer(captured.get("code_sequence"), 1)
            and captured.get("executable_sha256") == built["inputs"][
                "app" if kind == "rust" else "native_oracle"]["sha256"]
            and captured.get("classification") ==
                ("native-free" if kind == "rust" else "native-classic-dynamic")
            and environment.get("Fabric_NodeName") == rpc["node"]
            and environment.get("Fabric_ApplicationHostId") == rpc["host"]
            and environment.get("Fabric_CodePackageInstanceSeqNum") == str(captured["code_sequence"]),
            "original process action must bind native node/host/PID/executable/start")
    libraries = captured["native_libraries"]
    exact(libraries, ("fabricruntime", "fabriccommon", "fabricclient"), "process native library inventory")
    require(all(value is (kind != "rust") for value in libraries.values()), "process native library profile")
    return tuple(captured[key] for key in ("pid", "host", "start_ticks", "code_sequence", "executable_sha256"))


def validate_expected_process(expected, rpc, built):
    query = expected["native_query"]
    entry = query["MainEntryPoint"]
    require(same(expected.get("native_pid"), rpc["pid"]) and expected.get("host") == rpc["host"]
            and expected.get("node") == rpc["node"] and expected.get("kind") == rpc["engine_kind"]
            and expected.get("engine_sha256") == built["inputs"][
                "app" if rpc["engine_kind"] == "rust" else "native_oracle"]["sha256"]
            and query.get("Name") == "Code" and query.get("ServiceManifestName") == "RustRuntimeService"
            and text(expected.get("native_code_version")) and query.get("Version") == expected["native_code_version"]
            and entry.get("ProcessId") == str(rpc["pid"])
            and text(expected.get("native_entry_point_instance"))
            and entry.get("InstanceId") == expected["native_entry_point_instance"],
            "independent native code package identity must bind the actual process")


def validate_move(value, profile, service, partition):
    before, after = value["before"], value["after"]
    old, new = data_snapshot(before, profile, service), data_snapshot(after, profile, service)
    request = value["move_request"]
    require(before["partition"] == after["partition"] == partition and epoch(after) > epoch(before)
            and old["node"] != new["node"] and request.get("request") == "MovePrimary"
            and request.get("constraints") == "enforced" and request.get("native_request_completed") is True
            and request.get("partition") == before["partition"] and request.get("target") == new["node"]
            and any(r["role"] == "ActiveSecondary" and r["node"] == new["node"]
                    for r in before["replicas"])
            and type(value.get("old_primary_http_status")) is int and value["old_primary_http_status"] == 503,
            "actual native planned move/epoch/old-route evidence")
    surviving = [r for r in after["replicas"] if r["replica"] == old["replica"]]
    require(len(surviving) == 1 and surviving[0]["role"] == "ActiveSecondary"
            and replica_identity(surviving[0]["rpc"]) == replica_identity(old),
            "planned move must preserve and demote the original old-primary process")


def validate_siblings(value, old, process_loss):
    before, after = value["stateless_siblings_before"], value["stateless_siblings_after"]
    require(type(before) is list and type(after) is list and len(before) == len(after) >= 3,
            "actual colocated stateless sibling observations")
    previous, current = {}, {}
    for records, destination in ((before, previous), (after, current)):
        for record in records:
            rpc = record["rpc"]
            require(all(integer(rpc.get(key), 1) for key in ("instance", "incarnation", "pid"))
                    and all(text(rpc.get(key)) for key in ("partition", "node", "host"))
                    and same(record.get("native_pid"), rpc["pid"])
                    and integer(record.get("native_entry_point_instance"), 1)
                    and rpc["node"] not in destination, "native stateless sibling identity")
            destination[rpc["node"]] = record
    require(set(previous) == set(current) and old["node"] in previous
            and previous[old["node"]]["rpc"]["host"] == old["host"]
            and previous[old["node"]]["rpc"]["pid"] == old["pid"], "actual shared-process sibling binding")
    for node, original in previous.items():
        fresh = current[node]
        if process_loss and node == old["node"]:
            require(fresh["rpc"]["host"] != old["host"]
                    and fresh["native_entry_point_instance"] != original["native_entry_point_instance"],
                    "process-wide failure must replace its colocated stateless host")
        else:
            require(same(fresh, original), "per-unit/unrelated stateless identity must survive")
    for replica in value["observation"]["replicas"]:
        sibling = current.get(replica["node"])
        require(sibling is not None and sibling["rpc"]["host"] == replica["rpc"]["host"]
                and same(sibling["native_pid"], replica["rpc"]["pid"]),
                "recovered data/stateless units must bind the same native host")


def validate_contents(observation, results):
    """Bind the selected successful workload to independent provider contents."""
    values = {"key-" + key: "value-" + key for key in results}
    for rpc in [*(replica["rpc"] for replica in observation["replicas"]),
                observation["primary"]["rpc"]]:
        require(same(rpc["state"].get("results"), content_digest(results))
                and same(rpc["state"].get("values"), content_digest(values)),
                "observed providers and primary must preserve selected successful identities/LSNs/values")


def validate_post_writes(writes, before, observation, original_results, prefix):
    results = successful_writes(writes, 5)
    boundary = before["primary"]["rpc"]["state"]["last"]
    primary = observation["primary"]["rpc"]
    # Client success is quorum commitment, not the independent ALL-target
    # completion frontier, which can still await a peer's asynchronous ACK.
    require(all(key.startswith(prefix) for key in results)
            and not set(results) & set(original_results) and min(results.values()) > boundary
            and max(results.values()) == primary["state"]["last"]
            and all(primary["engine"]["progress"][key] >= max(results.values())
                    for key in ("assigned", "committed", "client_completed")),
            "post-movement/failure writes must actually advance successful primary progress")
    combined = {**original_results, **results}
    validate_contents(observation, combined)
    return combined


def validate_essential_data(value, profile, built):
    scenarios = value["scenarios"]
    baseline = scenarios["writes_100"]
    observation = baseline["observation"]
    primary = data_snapshot(observation, profile)
    service, partition = observation["service"], observation["partition"]
    processes = scenarios["deployment_identity"]["processes"]
    for replica in observation["replicas"]:
        matches = [process for process in processes
                   if process["native_identity"].get("node") == replica["node"]]
        require(len(matches) == 1, "baseline writes must bind the independently inspected deployment")
        validate_expected_process(matches[0]["native_identity"], replica["rpc"], built)
        validate_process(matches[0]["process"], replica["rpc"], built, "inspect")
    results = successful_writes(baseline["writes"], 100)
    workload = baseline["workload"]
    require(integer(workload.get("successful"), 100) and same(workload["successful"], len(results))
            and same(workload.get("accepted"), len(results)) and integer(workload.get("last"), 1)
            and workload["last"] == max(results.values()) and same(workload.get("results"), results)
            and workload.get("values") == {"key-" + key: "value-" + key for key in results},
            "100 successful writes must bind the independently converged workload")
    for replica in [*observation["replicas"], {"rpc": observation["primary"]["rpc"], "role": "Primary"}]:
        state, progress = replica["rpc"]["state"], replica["rpc"]["engine"]["progress"]
        require(same(state.get("results"), results) and same(state.get("values"), workload["values"])
                and same(state["last"], workload["last"])
                and progress["received"] >= state["last"]
                and (replica["role"] == "Primary" or progress["applied"] >= state["last"]),
                "independent provider state/order/receive/apply must match successful writes")
    require(all(primary["engine"]["progress"][key] >= workload["last"]
                for key in ("assigned", "committed", "client_completed")),
            "primary progress must substantiate actual successful commitments")
    for name in ESSENTIAL_SCENARIO_SPECS:
        scenario = scenarios[name]
        rpc = data_snapshot(scenario["observation"], profile, service)
        require(rpc["partition"] == partition, "data scenarios must bind the original partition")
        summary = scenario["workload"]
        require(integer(summary.get("successful"), 100) and integer(summary.get("accepted"), summary["successful"])
                and integer(summary.get("last"), summary["accepted"])
                and same(summary["last"], rpc["state"]["last"])
                and all(same(r["rpc"]["state"]["last"], summary["last"])
                        for r in scenario["observation"]["replicas"]), "converged scenario workload/progress")
    retry = scenarios["identified_retry"]
    duplicate_result(retry["identical_retry"], results)
    conflict = retry["conflicting_retry"]
    require(type(conflict.get("http_status")) is int and conflict["http_status"] in (400, 503)
            and conflict["reply"].get("outcome") == "rejected" and "sequence" not in conflict["reply"]
            and conflict["reply"].get("request_id") == retry["identical_retry"]["request_id"]
            and same(retry["workload"], {key: workload[key] for key in WORKLOAD_SPEC}),
            "conflicting retry must reject before admission without changing the workload")
    validate_contents(retry["observation"], results)
    planned = scenarios["planned_move"]
    validate_move(planned, profile, service, partition)
    for key in ("before", "after"):
        validate_contents(planned[key], results)
    moved_results = validate_post_writes(planned["post_move_writes"], planned["before"],
                                        planned["observation"], results, "planned-move-")

    lost = scenarios["lost_reply_dedup"]
    validate_move(lost, profile, service, partition)
    records = value["identified_workload"]
    require(type(records) is dict and len(records) == 1, "one actual intentionally unread identified write")
    request_id, record = next(iter(records.items()))
    require(record.get("id") == request_id and integer(record.get("sequence"), 1)
            and type(record.get("committed")) is bool, "actual lost-reply identity/sequence")
    lost_result = {request_id: record["sequence"]}
    attempts = record["attempts"]
    require(type(attempts) is list and len(attempts) >= 2
            and attempts[0].get("outcome") == "ambiguous_transport"
            and attempts[0].get("reply_intentionally_unread") is True
            and "reply" not in attempts[0], "actual sent/unread response transport attempt")
    for attempt in attempts[1:]:
        duplicate_result(attempt["reply"], lost_result)
        require(type(attempt.get("http_status")) is int
                and attempt["http_status"] == (200 if attempt["reply"]["outcome"] == "committed" else 202),
                "lost-reply duplicate HTTP outcome")
    require(record["committed"] is any(attempt["reply"]["outcome"] == "committed" for attempt in attempts[1:]),
            "lost-reply ledger must truthfully retain actual committed/ambiguous results")
    require(lost.get("retry_outcome") == attempts[1]["reply"]["outcome"], "lost-reply retry outcome binding")
    for key in ("before_move", "before", "after", "observation"):
        data_snapshot(lost[key], profile, service)
        require(lost[key]["partition"] == partition, "lost-reply observation partition binding")
        validate_contents(lost[key], {**moved_results, **lost_result})

    quorum = scenarios["unavailable_quorum"]
    for key in ("before", "observation"):
        validate_contents(quorum[key], {**moved_results, **lost_result})
    old = data_snapshot(quorum["before"], profile, service)
    replicas = quorum["before"]["replicas"]
    require(old["partition"] == partition and len({r["rpc"]["pid"] for r in replicas}) == 3,
            "quorum loss needs the original partition and three independent processes")
    paused, resumed = quorum["paused_processes"], quorum["resume"]
    require(type(paused) is list and type(resumed) is list and len(paused) == len(resumed) == 2,
            "both actual secondary processes must be paused and resumed")
    for index, secondary in enumerate(r for r in replicas if r["role"] == "ActiveSecondary"):
        require(validate_process(paused[index], secondary["rpc"], built, "pause") ==
                validate_process(resumed[index], secondary["rpc"], built, "resume"),
                "resume must bind the original paused process")
    native = quorum["native_while_unavailable"]
    partitions = native if type(native) is list else native["Items"]
    require(type(partitions) is list and len(partitions) == 1
            and type(partitions[0].get("TargetReplicaSetSize")) is int
            and partitions[0]["TargetReplicaSetSize"] == 3
            and type(partitions[0].get("MinReplicaSetSize")) is int
            and partitions[0]["MinReplicaSetSize"] == 2
            and partitions[0]["PartitionInformation"].get("Id") == partition,
            "native unavailable quorum cannot be manufactured by changing membership")
    write = quorum["write"]
    require(type(write.get("committed")) is bool and write["committed"] is False
            and text(write.get("id")) and same(write, quorum["accepted_write_outcome_after_recovery"]),
            "unavailable quorum must not manufacture a recovered success")
    attempts = write["attempts"]
    require(type(attempts) is list and len(attempts) == 2
            and attempts[0].get("native_quorum_unavailable") is True
            and attempts[0].get("outcome") == "acceptance_unknown_until_HTTP_reply",
            "actual unavailable quorum write attempt")
    reply = attempts[1]["reply"]
    require(reply.get("request_id") == write["id"], "unavailable quorum request identity")
    if reply.get("outcome") == "rejected":
        rejected_record({**write, "attempts": attempts[1:]})
    else:
        require(reply.get("outcome") == "ambiguous" and type(attempts[1].get("http_status")) is int
                and attempts[1]["http_status"] == 202 and integer(write.get("sequence"), 1)
                and same(reply.get("sequence"), write["sequence"])
                and write["sequence"] > old["engine"]["progress"]["committed"],
                "unavailable quorum permits only real ambiguity or pre-assignment rejection")
    diagnostics = quorum["diagnostics"]
    require(replica_identity(diagnostics) == replica_identity(old)
            and all(integer(diagnostics["engine"]["progress"].get(key)) for key in PROGRESS_SPEC)
            and diagnostics["engine"]["progress"]["committed"] == old["engine"]["progress"]["committed"]
            and (write["sequence"] is None or diagnostics["engine"]["progress"]["assigned"] >= write["sequence"]),
            "unavailable quorum must not advance committed progress from sends/receipts")

    failure = scenarios["abrupt_primary_loss"]
    old = quorum["observation"]["primary"]["rpc"]
    new = failure["observation"]["primary"]["rpc"]
    validate_expected_process(failure["native_identity"], old, built)
    require(validate_process(failure["native_identity"]["captured"], old, built, "inspect") ==
            validate_process(failure["kill"], old, built, "kill")
            and failure["kill"].get("old_process_exit_confirmed") is True
            and failure.get("process_wide_not_per_unit") is True
            and new["host"] != old["host"] and new["engine_incarnation"] != old["engine_incarnation"]
            and epoch(failure["observation"]) > epoch(quorum["observation"]),
            "abrupt failure must kill the bound original process and advance native epoch")
    validate_expected_process(failure["new_primary_native_identity"], new, built)
    validate_process(failure["new_primary_process"], new, built, "inspect")
    recovered_results = validate_post_writes(failure["post_failure_writes"], quorum["observation"],
                                            failure["observation"], {**moved_results, **lost_result},
                                            "abrupt-primary-loss-")
    replacements = failure["replacement_hosts"]
    require(type(replacements) is list and 1 <= len(replacements) <= 3, "actual replacement hosts required")
    replacement_ids = set()
    for replacement in replacements:
        expected = replacement["native_identity"]
        matches = [r["rpc"] for r in failure["observation"]["replicas"]
                   if r["rpc"]["host"] == expected["host"] and r["rpc"]["node"] == expected["node"]]
        require(len(matches) == 1 and expected["host"] != old["host"], "bound replacement native host")
        validate_expected_process(expected, matches[0], built)
        validate_process(replacement["process"], matches[0], built, "inspect")
        replacement_ids.add(replica_identity(matches[0]))
    require(len(replacement_ids) == len(replacements), "distinct replacement host observations")
    retries = failure["lost_reply_retries_after_recovery_copy"]
    require(type(retries) is list and len(retries) == 1, "lost-reply retry after replacement copy required")
    duplicate_result(retries[0], lost_result)
    # validate_post_writes bound every recovered provider's selected content,
    # including the original lost-response identity and its unchanged value.

    close = scenarios["per_unit_close"]
    before = data_snapshot(close["unrelated_before"], profile, service)
    after = data_snapshot(close["unrelated_after"], profile, service)
    auxiliary = close["auxiliary_before"]
    aux = data_snapshot(auxiliary, profile, target=1, minimum=1)
    require(replica_identity(before) == replica_identity(after)
            and before["partition"] == partition
            and replica_identity(after) == replica_identity(close["observation"]["primary"]["rpc"])
            and before["host"] == aux["host"] and before["pid"] == aux["pid"]
            and auxiliary["service"] != service and auxiliary["partition"] != partition
            and auxiliary["service"].rsplit("/", 1)[0] == service.rsplit("/", 1)[0]
            and type(close.get("old_auxiliary_http_status")) is int
            and close["old_auxiliary_http_status"] in (404, 503),
            "per-unit close must isolate a real colocated unit without replacing the main replica")
    preserved = {key: before["state"].get(key) for key in ("last", "results", "values")}
    for snapshot in (close["unrelated_before"], close["unrelated_after"], close["observation"]):
        validate_contents(snapshot, recovered_results)
    require(all(same({key: rpc["state"].get(key) for key in preserved}, preserved)
                for snapshot in (close["unrelated_before"], close["unrelated_after"], close["observation"])
                for rpc in [*(replica["rpc"] for replica in snapshot["replicas"]),
                            snapshot["primary"]["rpc"]]),
            "per-unit disposal must preserve the unrelated main provider boundary and retained results")
    departure = close["native_departure"]
    require(departure.get("observer") == "fube-client" and departure.get("fixture_contacted") is False
            and departure.get("service") == auxiliary["service"] and departure.get("node") == aux["node"]
            and type(departure.get("deployed")) is list
            and all(integer(item.get("replica_id"), 1) and item["replica_id"] != aux["replica"]
                    for item in departure["deployed"]), "independent native per-unit departure")
    if profile == "rust":
        validate_siblings(failure, old, True)
        validate_siblings(close, before, False)

    capacity = scenarios["ledger_capacity_backpressure"]
    rejected_record(capacity["capacity_rejection"], capacity=True)
    require(integer(capacity.get("new_successful_writes"), 1)
            and capacity["new_successful_writes"] <= 4096
            and same(capacity["workload"]["accepted"], 4096), "actual finite capacity workload")
    full = capacity["observation"]["primary"]["rpc"]["state"]
    full_results = full["results"]
    require(type(full_results) is dict and len(full_results) == 4096
            and all(text(key) and integer(sequence, 1) for key, sequence in full_results.items())
            and len(set(full_results.values())) == 4096
            and all(same(full_results.get(key), sequence) for key, sequence in recovered_results.items()),
            "finite capacity must retain 4096 actual unique identified results including old writes")
    require(capacity["capacity_rejection"]["id"] not in full_results,
            "capacity rejection must concern a genuinely new, unassigned identity")
    for replica in capacity["observation"]["replicas"]:
        state = replica["rpc"]["state"]
        require(type(state.get("history_count")) is int and state["history_count"] == 4096
                and integer(state.get("retained_bytes"), 1) and state["retained_bytes"] <= 16 * SMALL,
                "independently observed finite 4096-entry/16-MiB provider capacity")
        require(same(state.get("results"), content_digest(recovered_results))
                and same(state.get("values"), content_digest(
                    {"key-" + key: "value-" + key for key in recovered_results})),
                "every capacity provider must retain selected earlier successful contents")
    require(same(full.get("values"), content_digest(
                {"key-" + key: "value-" + key for key in recovered_results})),
            "capacity primary view must preserve selected successful values")
    duplicate_result(capacity["old_identity_retry_at_capacity"], results)


def validate_data(value, profile, built):
    required = data.required_scenarios(profile)
    require(value.get("profile") == profile and value.get("status") == "PASS"
            and value.get("sf_version") == "11.8.121.4"
            and value.get("errors") == [] and value.get("missing_or_failed") == []
            and value.get("required_scenarios") == list(required), "data complete truthful profile")
    exact(value["scenarios"], required, "data exact scenario inventory")
    times = {}
    for name, scenario in value["scenarios"].items():
        require(scenario.get("status") == "PASS", "data scenario failed/missing")
        times[name] = interval(scenario)
    require(value["bundle"]["source_revision"] == built["context"]["source"]
            and value["bundle"]["profile"] == data.BUNDLE_PROFILE
            and type(value["bundle"]["version"]) is int and value["bundle"]["version"] == data.BUNDLE_VERSION,
            "data strict bundle identity")
    source_provenance(value["provenance"], built)
    for artifact, key in (("bin/rust-runtime-app", "app"), ("bin/runtime_observer", "observer"),
                          ("bin/native-replicator-oracle", "native_oracle"),
                          ("legacy/build-info.json", "nested_manifest")):
        require(value["bundle"]["artifacts"][artifact]["sha256"] == built["inputs"][key]["sha256"],
                "data report strict executable/manifest digests")
    processes = value["scenarios"]["deployment_identity"]["processes"]
    require(type(processes) is list and len(processes) == 3, "three actual deployed data processes")
    for process in processes:
        native, captured = process["native_identity"], process["process"]
        kind = native["kind"]
        require(kind in ("rust", "native-classic-v1")
                and (profile != "rust" or kind == "rust"), "data deployed engine profile")
        key = "app" if kind == "rust" else "native_oracle"
        require(native.get("engine_sha256") == captured.get("executable_sha256")
                == built["inputs"][key]["sha256"]
                and captured.get("classification") ==
                ("native-free" if kind == "rust" else "native-classic-dynamic"),
                "actual deployed process executable identity")
        libraries = captured["native_libraries"]
        exact(libraries, {"fabricruntime", "fabriccommon", "fabricclient"}, "native library classification")
        require(all(item is (kind != "rust") for item in libraries.values()),
                "production Rust must remain native-free")
    if profile == "native-data":
        require(sorted(process["native_identity"]["kind"] for process in processes) ==
                ["native-classic-v1", "native-classic-v1", "rust"], "native-data actual mixed engine inventory")
    clean = value["cleanup"]
    require(clean.get("application") in ("deleted; absence confirmed", "absence confirmed")
            and clean.get("type") in ("unprovisioned; absence confirmed", "absence confirmed")
            and clean.get("upload") == "original reservation removed"
            and not any(key in clean for key in ("retained_for_recovery", "pending_native_fas", "deployment")),
            "logical application/type/staging cleanup required")
    absence(clean["image_store_upload"])
    require(clean.get("onebox") == "caller-owned allocation retained",
            "suite must retain original allocation for post-teardown observation")
    auxiliaries = value["auxiliary_data_services"]
    require(type(auxiliaries) is list and len(auxiliaries) == 2, "both data-loss auxiliary services required")
    expected_nodes = ["N0020", "N0040", "N0050"] if profile == "native-data" else list(data.NODE_ENGINES)
    for index, auxiliary in enumerate(auxiliaries):
        require(auxiliary.get("mixed") is False and auxiliary.get("eligible_nodes") == expected_nodes
                and auxiliary.get("changed") is bool(index) and auxiliary.get("target") == 3
                and type(auxiliary.get("target")) is int and auxiliary.get("minimum") == 2
                and type(auxiliary.get("minimum")) is int, "baseline full auxiliary placement policy")
        true_fields(auxiliary["cleanup"], ("native_absence_confirmed", "original_routes_retired"))
    validate_essential_data(value, profile, built)
    ack = value["scenarios"]["apply_ack_delay"]
    true_fields(ack, ("blocked_after_receive",))
    require(number(ack.get("elapsed_seconds"), 1.8), "actual ACK delay evidence")
    for name in ("replacement_copy", "copy_cancellation_recovery"):
        validate_copy(value["scenarios"][name])
    groups = value["scenarios"]["stale_wire_receipts"].get("coverage")
    require(type(groups) is list and [group.get("primary_kind") for group in groups]
            == (["rust", "native-classic-v1"] if profile == "native-data" else ["rust"]),
            "actual stale-wire source directions")
    cases = [case for group in groups for case in group["receipts"]]
    require(len(cases) == 6 and
            [(case.get("rust_role"), case.get("kind")) for case in cases] == [
                ("Primary", "stale_epoch"), ("Primary", "wrong_sender"), ("Primary", "wrong_target"),
                ("ActiveSecondary", "wrong_sender"), ("ActiveSecondary", "stale_copy_ack"),
                ("ActiveSecondary", "wrong_target")], "six independent invalid-frame ingress cases")
    for case in cases:
        correlation = case["peer_correlation"]
        delivery = correlation["wire_delivery"]
        require(delivery.get("classification") == "destination_kernel_ingress_not_native_parse_or_rejection"
                and sha(delivery.get("frame_sha256")) and integer(delivery.get("frame_bytes"), 1)
                and correlation.get("source_kind") == "rust"
                and correlation.get("destination_kind") == ("native-classic-v1" if profile == "native-data" else "rust"),
                "independent actual destination kernel ingress evidence")
    loss = value["scenarios"]["authoritative_data_loss"]
    for changed, name in ((False, "unchanged"), (True, "changed")):
        case = loss[name]
        true_fields(case["native_fas"], ("native_cleanup_terminal",))
        require(epoch(case["after_native_data_loss"])[0] > epoch(case["before"])[0],
                "actual native data-loss epoch must advance")
        callbacks = case["actual_survivor_callbacks"]
        require(type(callbacks) is list and callbacks, "original survivor callback observations required")
        for callback in callbacks:
            state = callback["state"]
            require(callback.get("engine_kind") == "rust" and integer(state.get("data_loss_calls"), 1)
                    and integer(state.get("data_loss_changed_calls"), 1 if changed else 0)
                    and state["data_loss_changed_calls"] <= state["data_loss_calls"]
                    and (state.get("last_data_loss_boundary") == 5
                         and type(state["last_data_loss_boundary"]) is int if changed else
                         state["data_loss_changed_calls"] == 0 and state.get("last_data_loss_boundary") is None),
                    "actual baseline auxiliary survivor callback/boundary evidence")
    if profile == "native-data":
        for name in data.MIXED_SCENARIOS[:2]:
            direction = value["scenarios"][name]
            true_fields(direction["apply_ack_delay"], ("blocked_after_receive",))
            validate_copy(direction["replacement_copy"], ack=True,
                          source_kind="rust" if name.startswith("rust_primary") else "native-classic-v1")
            moves = direction["post_copy_no_write_moves"]
            require(type(moves) is list and len(moves) == 4, "four post-copy no-write moves required")
            for move in moves:
                true_fields(move, ("no_intervening_client_writes",))
                require(type(move.get("old_primary_http_status")) is int
                        and move["old_primary_http_status"] == 503,
                        "native role movement must fence the surviving old-primary route")
                if move.get("requested_engine") == "rust":
                    peers = move.get("actual_fresh_native_ack_frontiers")
                    primary = move["last_native_observation"]["primary"]["rpc"]
                    boundary = primary["state"]["last"]
                    require(integer(boundary, 1) and primary.get("engine_kind") == "rust"
                            and primary["engine"]["progress"]["assigned"] == boundary
                            and type(primary["engine"]["progress"]["assigned"]) is int
                            and primary["engine"]["progress"]["committed"] == boundary
                            and type(primary["engine"]["progress"]["committed"]) is int
                            and type(peers) is list and len(peers) == 2
                            and all(integer(peer.get("replica"), 1) and integer(peer.get("received"), boundary)
                                    and integer(peer.get("applied"), boundary)
                                    and peer.get("building") is False and peer.get("cleanup_required") is False
                                    for peer in peers)
                            and len({peer["replica"] for peer in peers}) == 2,
                            "fresh actual native ACK frontiers required")
            require([move.get("requested_engine") for move in moves] ==
                    (["native-classic-v1", "rust"] * 2 if name.startswith("rust_primary")
                     else ["rust", "native-classic-v1"] * 2), "native no-write direction inventory")
        interrupted = value["scenarios"]["native_interrupted_copy_epoch"]
        validate_copy(interrupted)
        require(nonempty(interrupted.get("interrupt_move"))
                and epoch(interrupted["observation"]) > epoch(interrupted["before"]),
                "interrupted native copy must execute a real native epoch move")
    return {"allocation": report_binding(value), "timing": times,
            "assertions": [profile + "/" + name for name in required]}


def validate_native(reports, archive, allocation):
    clear, health, name, lists, notification = [reports[file] for file in NATIVE_FILES]
    for value in (clear, health, name, notification):
        require(value.get("profile") == "clear", "native clear profile required")
    for value in reports.values():
        require(value.get("runtime_version") == "11.8.121.4", "native runtime identity required")
    require(len({value["runtime_version"] for value in reports.values()}) == 1, "native runtime mismatch")
    for value in (health, name, lists, notification):
        require(value.get("archive_sha256") == archive, "actual native archive hash mismatch")
    cases = clear["executed_cases"]
    require(type(cases) is list and [case.get("case") for case in cases] == list(CLEAR_CASES),
            "clear seven internal cases are not five archived tests")
    for case in cases:
        true_fields(case, ("passed",))
    require(cases[1].get("result") is True and cases[2].get("result") is False
            and type(cases[3].get("operation_count")) is int and cases[3]["operation_count"] == 32
            and type(cases[3].get("initially_pending_count")) is int
            and cases[3]["initially_pending_count"] == 32, "clear native concurrency/name results")
    true_fields(cases[-1], ("native_ping_passed", "closed_without_operation", "terminal_error_observed"))
    nodes = clear["node_list"]
    require(type(nodes.get("native_node_count")) is int and nodes["native_node_count"] == 5,
            "native five-node observation")
    true_fields(nodes, ("typed_stable_fields_match", "binary_ports_match_onebox_manifest",
                       "exact_name_hit_miss_case_prefix", "status_default_all_up_down",
                       "empty_name_token_omitted", "valid_continuation_after_final_id_is_empty",
                       "connection_survives_native_error"))
    require([case.get("case") for case in health["cases"]] == list(HEALTH_CASES)
            and integer(health.get("concurrent_operations")) and health["concurrent_operations"] == 32
            and type(health.get("unknown_node_code")) is int and health["unknown_node_code"] == -2147017705
            and health.get("unknown_node_management_code") == "FABRIC_E_HEALTH_ENTITY_NOT_FOUND",
            "heterogeneous native health case/error inventory")
    true_fields(health, ("cancelled_after_first_poll",))
    for field in ("healthy_reuse_after_error", "final_healthy_reuse"):
        require(type(health[field].get("aggregated_health_state")) is int
                and health[field]["aggregated_health_state"] == 1, "native healthy reuse")
    require(integer(health.get("completed_at_unix_ms"), 1), "health completion endpoint")
    true_fields(name, ("initial_http_absent", "native_create_succeeded", "http_present_after_create",
                      "reuse_after_duplicate", "native_delete_succeeded", "http_absent_after_delete",
                      "reuse_after_missing", "shared_native_client", "owned_cleanup_confirmed"))
    require(type(name.get("duplicate_create_code")) is int and name["duplicate_create_code"] == -2147017792
            and type(name.get("missing_delete_code")) is int and name["missing_delete_code"] == -2147017791,
            "native name negative error codes")
    true_fields(lists, ("initial_absence_confirmed", "application_native_continuation_followed",
                       "service_native_continuation_followed", "filters_and_parameter_exclusion",
                       "mixed_shared_native_connection", "reuse_after_native_error", "owned_cleanup_confirmed"))
    require(type(lists.get("application_count")) is int and lists["application_count"] == 2
            and type(lists.get("service_count")) is int and lists["service_count"] == 4
            and all(nonempty(lists.get(key)) for key in ("topology", "deployment_inspection",
                    "independent_applications", "independent_services", "native_applications", "native_services")),
            "native application/service/topology/deployment observations")
    true_fields(notification, ("owned_cleanup_confirmed",))
    require(notification.get("allocation") == allocation["name"]
            and notification.get("passed") == list(NOTIFICATION_CASES)
            and all(nonempty(notification.get(key)) for key in
                    ("resolved", "named", "ack_retry", "endpoint_change", "role_change")),
            "native notification allocation/cases/actual observations")
    retry = notification["ack_retry"]
    require(all(integer(retry.get(key), 1) for key in
                ("successful_acks_dropped", "native_page_retries", "subsequent_target_acks_forwarded"))
            and type(retry.get("duplicate_versions")) is int and retry["duplicate_versions"] == 0,
            "actual native ACK loss/retry without duplicate delivery")
    windows = {
        "clear_node_queries": [stamp / 1000 for stamp in interval(
            nodes, "native_observation_start_unix_ms", "native_observation_end_unix_ms")],
    }
    require(all(integer(nodes.get(key), 1) for key in
                ("native_observation_start_unix_ms", "native_observation_end_unix_ms")),
            "native observation timestamps must be integer milliseconds")
    for case in health["cases"]:
        require(integer(case.get("native_start_ms"), 1) and integer(case.get("native_end_ms"), 1),
                "native health timestamps must be integer milliseconds")
        windows["health/" + case["case"]] = [
            stamp / 1000 for stamp in interval(case, "native_start_ms", "native_end_ms")]
    windows["health_report_completion"] = [health["completed_at_unix_ms"] / 1000] * 2
    return (["native-client/" + file.removesuffix("-report.json") for file in NATIVE_FILES], windows)


def fields(*names):
    return dict.fromkeys(names, True)


COPY_SPEC = {
    "live_copy": fields("commits_inside_observed_build_horizon_window"),
    "concurrent_writes": fields("successful"),
    "destination_copy_ack": {
        **fields("blocked_after_receive", "released", "negative_observations"),
        "held_operation": fields("sequence", "boundary"),
        "source_eos": {
            **fields("kind"),
            "peer": fields("replica", "copy_horizon", "copy_received", "copy_applied",
                           "copy_finish", "applied", "building"),
            "observation": fields("requested_horizon", "enumeration_end", "items",
                                  "begin_count", "end_count", "callback_count"),
        },
        "completed_native_replica": {
            **fields("replica", "status"),
            "rpc": {"copy_ack_hold": fields("released", "acknowledged", "held", "expired",
                                           "cancelled", "sequence", "boundary", "copy_end_received")}},
    },
}
EPOCH_SPEC = {"native_partition": {"epoch": fields("data_loss", "configuration")}}
LOSS_SPEC = {
    "native_fas": fields("native_cleanup_terminal"),
    "before": EPOCH_SPEC,
    "after_native_data_loss": EPOCH_SPEC,
    "actual_survivor_callbacks": {"*": {
        **fields("engine_kind"),
        "state": fields("data_loss_calls", "data_loss_changed_calls", "last_data_loss_boundary"),
    }},
}
SCENARIO_SPEC = {
    **fields("status", "started_at", "finished_at", "blocked_after_receive", "elapsed_seconds"),
    **COPY_SPEC,
    "apply_ack_delay": fields("blocked_after_receive"),
    "replacement_copy": COPY_SPEC,
    "post_copy_no_write_moves": {"*": {
        **fields("requested_engine", "no_intervening_client_writes", "old_primary_http_status"),
        "actual_fresh_native_ack_frontiers": {"*": fields("replica", "received", "applied", "building", "cleanup_required")},
        "last_native_observation": {"primary": {"rpc": {
            **fields("engine_kind"), "state": fields("last"),
            "engine": {"progress": fields("assigned", "committed")},
        }}},
    }},
    "coverage": {"*": {
        **fields("primary_kind"),
        "receipts": {"*": {
            **fields("rust_role", "kind"),
            "peer_correlation": {
                **fields("source_kind", "destination_kind"),
                "wire_delivery": fields("classification", "frame_sha256", "frame_bytes"),
            },
        }},
    }},
    "processes": {"*": {
        "native_identity": fields("kind", "engine_sha256"),
        "process": fields("executable_sha256", "classification", "native_libraries"),
    }},
    "before": EPOCH_SPEC,
    "observation": EPOCH_SPEC,
    "interrupt_move": True,
    "unchanged": LOSS_SPEC,
    "changed": LOSS_SPEC,
}
REPLY_SPEC = fields("request_id", "outcome", "sequence", "duplicate", "error")
RECORD_SPEC = {
    **fields("id", "key", "value", "sequence", "committed"),
    "attempts": {"*": {
        **fields("http_status", "outcome", "reply_intentionally_unread", "native_quorum_unavailable"),
        "reply": REPLY_SPEC,
    }},
}
LOST_RESULTS = PrefixSpec("lost-reply-", True)
# Keep every initial and post-movement success, not large later copy/capacity
# ledgers. Python's startswith accepts this tuple without changing the parser.
CONTENT_PREFIXES = ("initial-", "planned-move-", "abrupt-primary-loss-", "lost-reply-")
CONTENT_RESULTS = DigestSpec(PrefixSpec(CONTENT_PREFIXES, True))
CONTENT_VALUES = DigestSpec(PrefixSpec(tuple("key-" + prefix for prefix in CONTENT_PREFIXES), True))
PROGRESS_SPEC = fields("assigned", "received", "applied", "committed", "completed", "client_completed")
WORKLOAD_SPEC = fields("successful", "accepted", "last")
WRITES_SPEC = fields("requested", "successful", "ids", "sequences")
PROCESS_SPEC = {
    **fields("pid", "host", "start_ticks", "code_sequence", "action", "old_process_exit_confirmed",
             "executable_sha256", "classification", "native_libraries"),
    "activation_environment": fields("Fabric_NodeName", "Fabric_ApplicationHostId",
                                     "Fabric_CodePackageInstanceSeqNum"),
}
EXPECTED_PROCESS_SPEC = {
    **fields("native_pid", "native_entry_point_instance", "native_code_version", "host", "node",
             "kind", "engine_sha256"),
    "native_query": {
        **fields("Name", "Version", "ServiceManifestName"),
        "MainEntryPoint": fields("ProcessId", "InstanceId"),
    },
    "captured": PROCESS_SPEC,
}
SIBLING_SPEC = {"*": {
    **fields("native_pid", "native_entry_point_instance"),
    "rpc": fields("partition", "instance", "incarnation", "pid", "host", "node"),
}}


def snapshot_spec(results=LOST_RESULTS, values=None, primary_results=LOST_RESULTS):
    def rpc(selected):
        return {
            **fields("profile", "partition", "replica", "node", "incarnation", "engine_incarnation",
                     "host", "pid", "engine_kind", "role", "current_role", "service_address"),
            "state": {**fields("last", "copy_in_progress", "history_count", "retained_bytes"),
                      "results": selected, **({"values": values} if values is not None else {})},
            "engine": {**fields("closed", "fault", "copy_done"), "progress": PROGRESS_SPEC},
        }
    return {
        **fields("observer", "service", "partition"),
        "native_partition": {**fields("target", "minimum", "status"),
                             "epoch": fields("data_loss", "configuration")},
        "primary": {**fields("resolver", "endpoint", "native_pid"), "rpc": rpc(primary_results)},
        "replicas": {"*": {
            **fields("node", "replica", "role", "status", "address", "service_http_status"),
            "deployed": fields("host_process_id"), "rpc": rpc(results),
        }},
    }


SNAPSHOT_SPEC = snapshot_spec(results=CONTENT_RESULTS, values=CONTENT_VALUES,
                              primary_results=CONTENT_RESULTS)
CONTENT_SNAPSHOT_SPEC = SNAPSHOT_SPEC
MOVE_SPEC = {
    "before": SNAPSHOT_SPEC, "after": SNAPSHOT_SPEC,
    "move_request": fields("constraints", "native_request_completed", "partition", "request", "target"),
    **fields("old_primary_http_status"),
}
PARTITION_REST_SPEC = {
    **fields("TargetReplicaSetSize", "MinReplicaSetSize"),
    "PartitionInformation": fields("Id"),
}
ESSENTIAL_SCENARIO_SPECS = {
    "writes_100": {
        "writes": WRITES_SPEC,
        "workload": {**WORKLOAD_SPEC, **fields("results", "values")},
        "observation": snapshot_spec(results=True, values=True, primary_results=True),
    },
    "identified_retry": {
        "identical_retry": REPLY_SPEC,
        "conflicting_retry": {**fields("http_status"), "reply": REPLY_SPEC},
        "workload": WORKLOAD_SPEC, "observation": SNAPSHOT_SPEC,
    },
    "planned_move": {
        **MOVE_SPEC, "before": CONTENT_SNAPSHOT_SPEC, "after": CONTENT_SNAPSHOT_SPEC,
        "post_move_writes": WRITES_SPEC,
        "observation": CONTENT_SNAPSHOT_SPEC, "workload": WORKLOAD_SPEC,
    },
    "unavailable_quorum": {
        "before": SNAPSHOT_SPEC, "observation": SNAPSHOT_SPEC, "workload": WORKLOAD_SPEC,
        "paused_processes": {"*": PROCESS_SPEC}, "resume": {"*": PROCESS_SPEC},
        "native_while_unavailable": {"Items": {"*": PARTITION_REST_SPEC}, "*": PARTITION_REST_SPEC},
        "write": RECORD_SPEC, "accepted_write_outcome_after_recovery": RECORD_SPEC,
        "diagnostics": {
            **fields("partition", "replica", "node", "host", "pid", "incarnation", "engine_incarnation", "engine_kind"),
            "engine": {"progress": PROGRESS_SPEC},
        },
    },
    "lost_reply_dedup": {
        **MOVE_SPEC, "before_move": SNAPSHOT_SPEC, "observation": SNAPSHOT_SPEC,
        "workload": WORKLOAD_SPEC, **fields("retry_outcome"),
    },
    "abrupt_primary_loss": {
        "observation": CONTENT_SNAPSHOT_SPEC, "workload": WORKLOAD_SPEC,
        "native_identity": EXPECTED_PROCESS_SPEC, "kill": PROCESS_SPEC,
        "new_primary_native_identity": EXPECTED_PROCESS_SPEC, "new_primary_process": PROCESS_SPEC,
        "replacement_hosts": {"*": {"native_identity": EXPECTED_PROCESS_SPEC, "process": PROCESS_SPEC}},
        "post_failure_writes": WRITES_SPEC,
        "lost_reply_retries_after_recovery_copy": {"*": REPLY_SPEC},
        **fields("process_wide_not_per_unit"),
        "stateless_siblings_before": SIBLING_SPEC, "stateless_siblings_after": SIBLING_SPEC,
    },
    "per_unit_close": {
        **{key: CONTENT_SNAPSHOT_SPEC for key in ("observation", "unrelated_before", "unrelated_after")},
        "auxiliary_before": SNAPSHOT_SPEC,
        "workload": WORKLOAD_SPEC, **fields("old_auxiliary_http_status"),
        "native_departure": {
            **fields("observer", "node", "service", "fixture_contacted"),
            "deployed": {"*": fields("replica_id")},
        },
        "stateless_siblings_before": SIBLING_SPEC, "stateless_siblings_after": SIBLING_SPEC,
    },
    "ledger_capacity_backpressure": {
        "observation": snapshot_spec(results=CONTENT_RESULTS, values=CONTENT_VALUES, primary_results=True),
        "workload": WORKLOAD_SPEC,
        "capacity_rejection": RECORD_SPEC,
        "old_identity_retry_at_capacity": REPLY_SPEC, **fields("new_successful_writes"),
    },
}
DATA_SPEC = {
    **fields("profile", "status", "sf_version", "errors", "missing_or_failed", "required_scenarios", "onebox",
             "owner", "container_id", "provenance"),
    "bundle": {
        **fields("source_revision", "profile", "version"),
        "artifacts": {name: fields("sha256") for name in (
            "bin/rust-runtime-app", "bin/runtime_observer", "bin/native-replicator-oracle",
            "legacy/build-info.json")},
    },
    "cleanup": fields("application", "type", "upload", "image_store_upload", "onebox",
                      "retained_for_recovery", "pending_native_fas", "deployment"),
    "auxiliary_data_services": {"*": fields("mixed", "eligible_nodes", "changed", "target",
                                         "minimum", "cleanup")},
    "identified_workload": PrefixSpec("lost-reply-", RECORD_SPEC),
    "scenarios": {"*": SCENARIO_SPEC, "deployment_identity": {
        **SCENARIO_SPEC, "processes": {"*": {
            "native_identity": EXPECTED_PROCESS_SPEC, "process": PROCESS_SPEC,
        }},
    }, **{
        name: {**SCENARIO_SPEC, **spec} for name, spec in ESSENTIAL_SCENARIO_SPECS.items()
    }},
}
LEGACY_SPEC = {
    **fields("version", "kind", "profile", "status", "errors", "allocation", "context",
             "producer_sha256", "provenance"),
    "scenarios": {"*": {
        **fields("status", "started_at", "finished_at"),
        "qualification": {
            **fields("onebox", "owner", "container_id", "sf_version", "status", "errors", "cleanup",
                     "executable_source_revision", "bundle_manifest_sha256", "binary_sha256",
                     "deployment_versions", "deployment_outcomes", "original_application_identity"),
            **{name: {} for name in ("stateful_singleton", "stateful_three", "resilience",
                                    "restart", "stateful_after_legacy_recovery", "configuration_upgrade")},
        },
    }},
}


def inventory(route):
    result = {"cleanup.json", "validated-input.json", "suite-outcomes.json", "fixture.json", "diagnostics.json"}
    for suite in suites.MANIFEST.selected(route).values():
        result.update(suite["reports"] if suite["kind"] == "nextest" else [suite["report"]])
    return result


def outcomes(value, route, archive):
    exact(value, OUTCOMES, "exact worker command outcome keys")
    active = {"validate", "create", "destroy"} | suites.MANIFEST.selected(route).keys()
    for key in OUTCOMES:
        suite = ONEBOX_SUITES.get(key)
        if key in active and suite is not None and suite["kind"] == "nextest":
            require(same(value[key], {"outcome": "success", "archive_sha256": archive,
                                     "filter": suite["filter"], "no_tests": "fail"}),
                    "native archive execution/nonempty selection")
        else:
            require(value[key] == ("success" if key in active else "skipped"),
                    "failed/skipped/unexecuted worker command")


def inspect_worker(route, built, producer_digest, validated_path, directory, command_outcomes, current):
    outcomes(command_outcomes, route, built["inputs"]["native_archive"]["sha256"])
    directory = safe_path(directory)
    require(directory.is_dir(), "worker evidence directory required")
    expected = inventory(route)
    entries = list(directory.rglob("*"))
    require(len(entries) <= 32 and all(not entry.is_symlink() and (entry.is_dir() or entry.is_file())
                                     for entry in entries), "bounded regular worker inventory")
    actual = {entry.relative_to(directory).as_posix() for entry in entries if entry.is_file()}
    expected_directories = {parent.as_posix() for name in expected for parent in Path(name).parents
                            if parent != Path(".")}
    require({entry.relative_to(directory).as_posix() for entry in entries if entry.is_dir()}
            == expected_directories, "route fixed report directory inventory")
    require(expected <= actual and actual <= expected | {"worker-proof.json", "emergency-cleanup.json"},
            "route fixed report inventory")
    require(safe_path(validated_path) == directory / "validated-input.json",
            "validated input must be published under fixed worker filename")
    validated = receipt(validated_path, "validated-input", current)
    exact(validated, {"version", "kind", "status", "errors", "route", "context", "inputs",
                      "producer_sha256", "validated_at"}, "validated-input schema")
    require(validated["route"] == route and same(validated["inputs"], built["inputs"])
            and validated["producer_sha256"] == producer_digest
            and number(validated["validated_at"], 1), "validated input/producer identity")
    cleanup_path = directory / "cleanup.json"
    clean = receipt(cleanup_path, "cleanup", current)
    exact(clean, {"version", "kind", "status", "errors", "route", "context", "producer_sha256",
                  "allocation", "destroy", "started_at", "finished_at"}, "cleanup receipt schema")
    require(clean["route"] == route and clean["producer_sha256"] == producer_digest,
            "post-teardown cleanup producer/route")
    exact(clean["allocation"], {"name", "owner_sha256"}, "cleanup original allocation schema")
    require(sha(clean["allocation"]["owner_sha256"]), "cleanup original owner digest")
    resources = validate_destroy(clean)
    execution = read(directory / "suite-outcomes.json", SMALL)
    exact(execution, {"version", "partition", "manifest_sha256", "suites", "status", "errors"},
          "suite execution schema")
    require(type(execution["version"]) is int and execution["version"] == 1
            and execution["partition"] == route and execution["status"] == "PASS"
            and execution["errors"] == []
            and execution["manifest_sha256"] == built["inputs"]["suite_manifest"]["sha256"],
            "actual partition execution/manifest binding")
    exact(execution["suites"], suites.MANIFEST.selected(route), "actual selected suite inventory")
    require(list(execution["suites"]) == list(suites.MANIFEST.selected(route)), "actual suite execution order")
    require(all(same(result, command_outcomes[name]) for name, result in execution["suites"].items()),
            "partition/worker execution outcome mismatch")
    original = {**clean["allocation"], "container_id": resources["container"][0]}
    lifecycle = receipt(directory / "fixture.json", "onebox-fixture", current)
    exact(lifecycle, {"version", "kind", "status", "errors", "route", "context", "producer_sha256",
                      "allocation", "ownership", "outcomes", "started_at", "ready_at", "finished_at"},
          "onebox fixture schema")
    exact(lifecycle["outcomes"], FIXTURE_PHASES, "onebox fixture phase inventory")
    require(lifecycle["route"] == route and lifecycle["producer_sha256"] == producer_digest
            and lifecycle["ownership"] == "created" and same(lifecycle["allocation"], original)
            and all(value == "success" for value in lifecycle["outcomes"].values())
            and all(lifecycle["outcomes"][key] == command_outcomes[key]
                    for key in ("validate", "create", "destroy")), "owned fixture readiness/lifecycle binding")
    fixture_window = interval(lifecycle)
    require(number(lifecycle["ready_at"], 1)
            and fixture_window[0] <= validated["validated_at"] <= lifecycle["ready_at"]
            <= clean["started_at"] <= clean["finished_at"] <= fixture_window[1],
            "fixture validation/readiness/teardown ordering")
    diagnostic = read(directory / "diagnostics.json", SMALL)
    require(diagnostic.get("name") == original["name"]
            and diagnostic.get("project") == "sf-onebox-" + original["name"]
            and diagnostic.get("errors") == [], "owned credential-safe diagnostics")
    summaries = []
    native_assertions, native_windows = [], {}
    for suite in suites.MANIFEST.selected(route).values():
        if suite["kind"] == "data":
            report = read(directory / suite["report"], REPORT, DATA_SPEC)
            summaries.append(validate_data(report, suite["profile"], built))
        elif suite["kind"] == "legacy":
            preserved = read(directory / suite["report"], REPORT, LEGACY_SPEC)
            require(preserved.get("status") == "PASS" and same(preserved.get("context"), current)
                    and preserved.get("producer_sha256") == producer_digest, "hardened legacy receipt binding")
            summaries.append(validate_legacy(preserved, built))
        else:
            native = {Path(file).name: read(directory / file, REPORT) for file in suite["reports"]}
            assertions, windows = validate_native(native, built["inputs"]["native_archive"]["sha256"], original)
            native_assertions.extend(assertions)
            native_windows.update(windows)
            require(native[NATIVE_FILES[1]].get("owner") == "ci-" + current["run_id"] + "-" + current["attempt"],
                    "native client run/attempt fixture owner")
    require(all(same(summary["allocation"], original) for summary in summaries),
            "all suite reports must match independent original cleanup")
    suite_times = {}
    for summary in summaries:
        timing = summary["timing"]
        if type(timing) is list:
            suite_times["legacy"] = timing
        else:
            suite_times.update(timing)
        endpoints = [timing] if type(timing) is list else timing.values()
        require(all(lifecycle["ready_at"] <= start <= end <= clean["started_at"]
                    for start, end in endpoints), "suite execution must precede original teardown")
    require(all(lifecycle["ready_at"] <= start <= end <= clean["started_at"]
                for start, end in native_windows.values()), "native observations must belong to this worker interval")
    hashes = {name: identity(directory / name, REPORT if name.endswith("-report.json") else SMALL)
              for name in sorted(actual - {"worker-proof.json"})}
    return {
        "context": current, "route": route, "producer_sha256": producer_digest, "inputs": built["inputs"],
        "allocation": original, "resources": resources, "outcomes": command_outcomes, "reports": hashes,
        "assertions": [item for summary in summaries for item in summary["assertions"]],
        "native_tests": native_assertions,
        "timing": {"validated_at": validated["validated_at"], "fixture": fixture_window,
                   "ready_at": lifecycle["ready_at"], "suites": suite_times,
                   "native_observations": native_windows,
                   "cleanup": interval(clean)},
    }


def seal(args):
    started = time.time()
    current = context()
    built = producer(args.producer, current)
    if args.outcomes is None:
        lifecycle = read(args.evidence_dir / "fixture.json", SMALL)
        command_outcomes = {key: lifecycle["outcomes"][key] for key in ("validate", "create", "destroy")}
    else:
        command_outcomes = small_json(args.outcomes)
    if set(command_outcomes) == {"validate", "create", "destroy"}:
        execution = read(args.evidence_dir / "suite-outcomes.json", SMALL)
        command_outcomes = {key: "skipped" for key in OUTCOMES} | command_outcomes | execution["suites"]
    result = inspect_worker(args.route, built, identity(args.producer, SMALL)["sha256"],
                            args.validated_input, args.evidence_dir, command_outcomes, current)
    finished = time.time()
    require(result["timing"]["fixture"][1] <= started, "sealing requires completed fixture teardown")
    result["timing"].update(sealing=[started, finished],
                            worker_elapsed_seconds=finished - result["timing"]["validated_at"])
    return result


def aggregate(args):
    started = time.time()
    current = context()
    built = producer(args.producer, current)
    producer_digest = identity(args.producer, SMALL)["sha256"]
    needs = small_json(args.needs_json)
    require(needs == dict.fromkeys(REQUIRED_JOBS, "success"), "all required CI jobs must succeed")
    attempts = (small_json(args.worker_attempts_json) if args.worker_attempts_json is not None
                else dict.fromkeys(ROUTES, current["attempt"]))
    exact(attempts, ROUTES, "exact manifest worker attempts required")
    worker_contexts = {route: attempt_context(current, attempts[route]) for route in ROUTES}
    require(all(int(attempt) >= int(built["context"]["attempt"]) for attempt in attempts.values()),
            "worker predates selected producer")
    root = safe_path(args.worker_root)
    require(root.is_dir() and {entry.name for entry in root.iterdir()} == set(ROUTES)
            and all(safe_path(root / route).is_dir() for route in ROUTES), "exact manifest worker routes required")
    workers, proofs = {}, {}
    for route in ROUTES:
        directory = root / route
        proof_path = directory / "worker-proof.json"
        worker_current = worker_contexts[route]
        proof = receipt(proof_path, "worker-proof", worker_current)
        require(proof.get("route") == route and proof.get("producer_sha256") == producer_digest,
                "worker route/producer identity")
        expected = inspect_worker(route, built, producer_digest, directory / "validated-input.json",
                                  directory, proof["outcomes"], worker_current)
        require(set(proof) == set(expected) | {"version", "kind", "status", "errors"},
                "worker proof exact schema")
        for key in expected:
            if key != "timing":
                require(same(proof[key], expected[key]), "sealed worker report hash/proof mismatch")
        timing = proof["timing"]
        require(set(timing) == set(expected["timing"]) | {"sealing", "worker_elapsed_seconds"}
                and all(same(timing[key], value) for key, value in expected["timing"].items()),
                "worker actual timing binding")
        window = timing["sealing"]
        require(type(window) is list and len(window) == 2 and all(number(t, 1) for t in window)
                and timing["cleanup"][1] <= window[0] <= window[1]
                and number(timing["worker_elapsed_seconds"])
                and timing["worker_elapsed_seconds"] == window[1] - timing["validated_at"],
                "worker elapsed/sealing timing")
        workers[route], proofs[route] = proof, identity(proof_path, SMALL)
    for first, second in combinations(workers.values(), 2):
        for key in ("name", "owner_sha256", "container_id"):
            require(first["allocation"][key] != second["allocation"][key],
                    "workers require distinct original allocations")
        for kind in ("container", "volume", "network"):
            require(not set(first["resources"][kind]) & set(second["resources"][kind]),
                    "workers require independent original resources")
    assertions = [item for route in ROUTES for item in workers[route]["assertions"]]
    native_tests = [item for route in ROUTES for item in workers[route]["native_tests"]]
    expected_assertions, expected_native = suites.MANIFEST.coverage()
    require(assertions == expected_assertions and native_tests == expected_native,
            "exact manifest whole-suite coverage")
    return {"context": current, "producer_context": built["context"], "worker_contexts": worker_contexts,
            "producer_sha256": producer_digest, "inputs": built["inputs"],
            "needs": needs, "workers": proofs, "assertions": assertions, "native_tests": native_tests,
            "coverage": f"{len(assertions)} namespaced profile assertions; archived native-client{len(native_tests)}",
            "timing": {"aggregate": [started, time.time()],
                       "workers": {route: workers[route]["timing"] for route in ROUTES}}}


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    for command in ("produce", "verify-artifact", "validate", "legacy", "cleanup", "seal", "aggregate"):
        sub = commands.add_parser(command)
        if command in ("produce", "validate"):
            sub.add_argument("--bundle-tar", required=True, type=Path)
            sub.add_argument("--bundle-dir", required=True, type=Path)
            sub.add_argument("--archive", required=command == "produce", type=Path)
        if command == "produce":
            sub.add_argument("--standalone-bundle", required=True, type=Path)
        if command == "verify-artifact":
            sub.add_argument("--input", required=True, choices=("native_archive", "standalone_bundle"))
            sub.add_argument("--file", required=True, type=Path)
        if command == "validate":
            sub.add_argument("--receipt", required=True, type=Path)
        if command in ("verify-artifact", "legacy", "cleanup", "seal", "aggregate"):
            sub.add_argument("--producer", required=True, type=Path)
        if command in ("legacy", "cleanup"):
            sub.add_argument("--name", required=True)
            sub.add_argument("--owner", required=True)
            sub.add_argument("--report", required=True, type=Path)
        else:
            sub.add_argument("--out", required=True, type=Path)
        if command in ("validate", "cleanup", "seal"):
            sub.add_argument("--route", choices=ROUTES, required=True)
        if command == "legacy":
            sub.add_argument("--prebuilt", required=True, type=Path)
        if command == "seal":
            sub.add_argument("--validated-input", required=True, type=Path)
            sub.add_argument("--evidence-dir", required=True, type=Path)
            sub.add_argument("--outcomes", help="optional explicit outcomes, still checked against fixture evidence")
        if command == "aggregate":
            sub.add_argument("--worker-root", required=True, type=Path)
            sub.add_argument("--needs-json", required=True)
            sub.add_argument("--worker-attempts-json",
                             help="exact worker attempts from dependency evidence outputs")
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    kind = {"produce": "producer", "validate": "validated-input",
            "seal": "worker-proof"}.get(args.command, args.command)
    result = {"version": 1, "kind": kind, "status": "FAIL", "errors": []}
    destination = args.report if args.command in ("legacy", "cleanup") else args.out
    file, reservation_error = None, False
    try:
        file = reserve(destination, getattr(args, "prebuilt", getattr(args, "bundle_dir", None)))
    except Exception:
        reservation_error = True
        result["errors"].append("NEW output reservation failed")
    try:
        if not reservation_error or args.command == "cleanup":
            value = globals()[args.command.replace("-", "_")](args)
            result.update(value)
            result["status"] = "FAIL" if result["errors"] or reservation_error else "PASS"
    except (Exception, KeyboardInterrupt) as error:
        result["errors"].append(str(error) if isinstance(error, ProofError)
                                else "proof operation failed: " + type(error).__name__)
    if reservation_error:
        result["status"] = "FAIL"
        if "NEW output reservation failed" not in result["errors"]:
            result["errors"].append("NEW output reservation failed")
    if file is not None:
        try:
            write(file, result, REPORT if args.command == "legacy" else SMALL)
        except Exception:
            result["status"] = "FAIL"
            result["errors"] = ["evidence publication failed"]
        finally:
            file.close()
    # Never print report bodies, raw errors, owner capabilities or runtime logs.
    print(encode({"kind": kind, "status": result["status"],
                  "errors": [] if result["status"] == "PASS" else ["qualification proof failed"]}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
