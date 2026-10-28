#!/usr/bin/env python3
"""Compiler-free CI receipts for the two whole-suite onebox routes.

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
import re
import stat
import subprocess
import sys
import tarfile
import time

import fube_data_test as data

runtime = data.runtime
ROOT = data.ROOT
SMALL = 1024 * 1024
REPORT = 512 * SMALL
INPUT = 4 * 1024 * SMALL
ROUTES = ("client-runtime", "native-data")
OUTCOMES = {"validate", "create", "native_clients", "rust_data", "legacy", "native_data", "destroy"}
NATIVE_FILES = (
    "native-client-report.json", "native-health-report.json", "native-name-report.json",
    "native-list-report.json", "native-notification-report.json",
)
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
        require(depth <= 96, "JSON nesting bound")
        char = self.peek()
        require(char, "truncated JSON")
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
        "qualification_script": identity(ROOT / "scripts/fube_data_test.py"),
        "runtime_script": identity(ROOT / "scripts/rust_runtime_test.py"),
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


def producer(path, current):
    value = receipt(path, "producer", current)
    exact(value, {"version", "kind", "status", "errors", "context", "inputs", "created_at"},
          "producer receipt schema")
    exact(value["inputs"], {"bundle_tar", "manifest", "nested_manifest", "app", "observer",
                           "native_oracle", "native_archive", "qualification_script", "runtime_script"},
          "producer input inventory")
    for item in value["inputs"].values():
        exact(item, {"sha256", "bytes"}, "input identity schema")
        require(sha(item["sha256"]) and integer(item["bytes"], 1) and item["bytes"] <= INPUT,
                "input identity hash/size")
    require(number(value["created_at"], 1), "producer timestamp")
    return value


def produce(args):
    current = context()
    bundle, inputs = bundle_identity(args.bundle_tar, args.bundle_dir, args.archive)
    require(bundle.source_revision == current["source"], "bundle is not actual checkout source")
    require(same(context(), current), "source changed during input verification")
    return {"context": current, "inputs": inputs, "created_at": time.time()}


def validate(args):
    current = context()
    built = producer(args.receipt, current)
    require(args.route != "client-runtime" or args.archive is not None, "client route requires archive")
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


def validate_copy(value, ack=False):
    require(value.get("live_copy", {}).get("commits_inside_observed_build_horizon_window", 0) >= 100
            and integer(value["live_copy"]["commits_inside_observed_build_horizon_window"], 100)
            and integer(value.get("concurrent_writes", {}).get("successful"), 100),
            "native copy must overlap at least 100 actual writes")
    if ack:
        gate = value["destination_copy_ack"]
        true_fields(gate, ("blocked_after_receive", "released"))
        require(integer(gate.get("negative_observations"), 2) and nonempty(gate.get("source_eos")),
                "native copy independent receive-before-ACK negative gate")
        hold = gate["completed_native_replica"]["rpc"]["copy_ack_hold"]
        true_fields(hold, ("released", "acknowledged"))
        require(all(hold.get(field) is False for field in ("held", "expired", "cancelled")),
                "native copy hold did not complete safely")


def epoch(value):
    item = value["native_partition"]["epoch"]
    exact(item, {"data_loss", "configuration"}, "actual native epoch schema")
    require(all(integer(part) for part in item.values()), "native epoch integer fields")
    return item["data_loss"], item["configuration"]


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
            validate_copy(direction["replacement_copy"], ack=True)
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
        "source_eos": fields("kind"),
        "completed_native_replica": {"rpc": {"copy_ack_hold": fields(
            "released", "acknowledged", "held", "expired", "cancelled")}},
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
    "scenarios": {"*": SCENARIO_SPEC},
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
    common = {"cleanup.json", "validated-input.json"}
    if route == "client-runtime":
        return common | {"rust-report.json", "legacy-report.json"} | {
            "client/" + file for file in NATIVE_FILES}
    return common | {"native-data-report.json"}


def outcomes(value, route, archive):
    exact(value, OUTCOMES, "exact worker command outcome keys")
    active = ({"validate", "create", "native_clients", "rust_data", "legacy", "destroy"}
              if route == "client-runtime" else {"validate", "create", "native_data", "destroy"})
    for key in OUTCOMES - {"native_clients"}:
        require(value[key] == ("success" if key in active else "skipped"),
                "failed/skipped/unexecuted worker command")
    if route == "client-runtime":
        exact(value["native_clients"], {"outcome", "archive_sha256", "filter", "no_tests"},
              "native archive command receipt")
        require(value["native_clients"] == {"outcome": "success", "archive_sha256": archive,
                "filter": "group(onebox)", "no_tests": "fail"}, "native archive execution/nonempty selection")
    else:
        require(value["native_clients"] == "skipped", "unexpected native client execution route")


def inspect_worker(route, built, producer_digest, validated_path, directory, command_outcomes):
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
    require(expected <= actual and actual <= expected | {"diagnostics.json", "worker-proof.json"},
            "route fixed report inventory")
    require(safe_path(validated_path) == directory / "validated-input.json",
            "validated input must be published under fixed worker filename")
    current = built["context"]
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
    summaries = []
    if route == "client-runtime":
        rust = read(directory / "rust-report.json", REPORT, DATA_SPEC)
        summaries.append(validate_data(rust, "rust", built))
        preserved = read(directory / "legacy-report.json", REPORT, LEGACY_SPEC)
        require(preserved.get("status") == "PASS" and same(preserved.get("context"), current)
                and preserved.get("producer_sha256") == producer_digest, "hardened legacy receipt binding")
        summaries.append(validate_legacy(preserved, built))
        native = {file: read(directory / "client" / file, REPORT) for file in NATIVE_FILES}
        native_assertions, native_windows = validate_native(
            native, built["inputs"]["native_archive"]["sha256"], summaries[0]["allocation"])
        health_owner = native["native-health-report.json"].get("owner")
        require(health_owner == "ci-" + current["run_id"] + "-" + current["attempt"],
                "native client run/attempt fixture owner")
    else:
        native_data = read(directory / "native-data-report.json", REPORT, DATA_SPEC)
        summaries.append(validate_data(native_data, "native-data", built))
        native_assertions, native_windows = [], {}
    original = summaries[0]["allocation"]
    require(all(same(summary["allocation"], original) for summary in summaries)
            and same({key: original[key] for key in ("name", "owner_sha256")}, clean["allocation"])
            and resources["container"] == [original["container_id"]],
            "all suite reports must match independent original cleanup")
    suite_times = {}
    for summary in summaries:
        timing = summary["timing"]
        if type(timing) is list:
            suite_times["legacy"] = timing
        else:
            suite_times.update(timing)
        endpoints = [timing] if type(timing) is list else timing.values()
        require(all(validated["validated_at"] <= start <= end <= clean["started_at"]
                    for start, end in endpoints), "suite execution must precede original teardown")
    require(all(validated["validated_at"] <= start <= end <= clean["started_at"]
                for start, end in native_windows.values()), "native observations must belong to this worker interval")
    hashes = {name: identity(directory / name, REPORT if name.endswith("-report.json") else SMALL)
              for name in sorted(actual - {"worker-proof.json"})}
    return {
        "context": current, "route": route, "producer_sha256": producer_digest, "inputs": built["inputs"],
        "allocation": original, "resources": resources, "outcomes": command_outcomes, "reports": hashes,
        "assertions": [item for summary in summaries for item in summary["assertions"]],
        "native_tests": native_assertions,
        "timing": {"validated_at": validated["validated_at"], "suites": suite_times,
                   "native_observations": native_windows,
                   "cleanup": interval(clean)},
    }


def seal(args):
    started = time.time()
    current = context()
    built = producer(args.producer, current)
    result = inspect_worker(args.route, built, identity(args.producer, SMALL)["sha256"],
                            args.validated_input, args.evidence_dir, small_json(args.outcomes))
    finished = time.time()
    require(result["timing"]["cleanup"][1] <= started, "sealing requires completed teardown")
    result["timing"].update(sealing=[started, finished],
                            worker_elapsed_seconds=finished - result["timing"]["validated_at"])
    return result


def aggregate(args):
    started = time.time()
    current = context()
    built = producer(args.producer, current)
    producer_digest = identity(args.producer, SMALL)["sha256"]
    needs = small_json(args.needs_json)
    require(needs == {"producer": "success", "workers": "success"}, "all producer/worker jobs must succeed")
    root = safe_path(args.worker_root)
    require(root.is_dir() and {entry.name for entry in root.iterdir()} == set(ROUTES)
            and all(safe_path(root / route).is_dir() for route in ROUTES), "exact two worker routes required")
    workers, proofs = {}, {}
    for route in ROUTES:
        directory = root / route
        proof_path = directory / "worker-proof.json"
        proof = receipt(proof_path, "worker-proof", current)
        require(proof.get("route") == route and proof.get("producer_sha256") == producer_digest,
                "worker route/producer identity")
        expected = inspect_worker(route, built, producer_digest, directory / "validated-input.json",
                                  directory, proof["outcomes"])
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
    first, second = [workers[route] for route in ROUTES]
    for key in ("name", "owner_sha256", "container_id"):
        require(first["allocation"][key] != second["allocation"][key], "workers require distinct original allocations")
    for kind in ("container", "volume", "network"):
        require(not set(first["resources"][kind]) & set(second["resources"][kind]),
                "workers require independent original resources")
    assertions = [item for route in ROUTES for item in workers[route]["assertions"]]
    native_tests = first["native_tests"] + second["native_tests"]
    expected_assertions = ["rust/" + name for name in data.COMMON_SCENARIOS] + ["legacy/legacy_full"] + [
        "native-data/" + name for name in (*data.COMMON_SCENARIOS, *data.MIXED_SCENARIOS)]
    require(assertions == expected_assertions and len(assertions) == 32 and len(set(assertions)) == 32
            and len(native_tests) == 5 and len(set(native_tests)) == 5, "exact namespaced 32+5 whole-suite coverage")
    return {"context": current, "producer_sha256": producer_digest, "inputs": built["inputs"],
            "needs": needs, "workers": proofs, "assertions": assertions, "native_tests": native_tests,
            "coverage": "Rust14 + native-data17 + hardened legacy1; archived native-client5",
            "timing": {"aggregate": [started, time.time()],
                       "workers": {route: workers[route]["timing"] for route in ROUTES}}}


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    for command in ("produce", "validate", "legacy", "cleanup", "seal", "aggregate"):
        sub = commands.add_parser(command)
        if command in ("produce", "validate"):
            sub.add_argument("--bundle-tar", required=True, type=Path)
            sub.add_argument("--bundle-dir", required=True, type=Path)
            sub.add_argument("--archive", required=command == "produce", type=Path)
        if command == "validate":
            sub.add_argument("--receipt", required=True, type=Path)
        if command in ("legacy", "cleanup", "seal", "aggregate"):
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
            sub.add_argument("--outcomes", required=True)
        if command == "aggregate":
            sub.add_argument("--worker-root", required=True, type=Path)
            sub.add_argument("--needs-json", required=True)
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
            value = globals()[args.command](args)
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
