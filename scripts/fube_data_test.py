#!/usr/bin/env python3
"""Compiler-free, native-managed volatile-data qualification (not a simulator).

Build an immutable bundle upstream from clean source. Validate it before acquiring
any allocation capability. Native SF alone controls membership, roles and epochs.
Missing scenarios are failures, even when the implemented scenarios pass.

Uploads use the shipped native SDK azurecliproxy apphost, one regular file per
CopyApplicationPackage/UploadContent call, never sfctl's directory walker. Source
contract (read-only inspection): microsoft/service-fabric commit
51a7703a1f83749a9234d3ceb4296d8622e42cca, src/prod/src/managed/azurecliproxy/
AzureCliProxy.cs:24-40, ImageStore/NativeImageStoreClient.cs:436-456 under the
managed Api, and Management/ImageStore/NativeImageStore.cpp:619-747,1475-1507.
UploadContent selects UploadFileAsyncOperation for File::Exists(localSource)
and preserves the explicit remote-relative filename. NativeImageStore.cpp:496-533
requires directory _.dir markers; these use one pinned, empty regular-file FD,
with the package-root marker uploaded last. The pinned 11.8.121.4
runtime deb (SHA-256 fbcb0b8d52e84b7e2bf9c336d9e0ece8cb9ff7fa1de53367d56c6ff8ab850c83)
contains Fabric.Code/azurecliproxy and azurecliproxy.dll. Invoke the apphost
directly: its shell wrapper changes coredump_filter. No wrapper, COM shim,
directory-upload fallback, permission change, or native qualification is implied.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
from dataclasses import dataclass, replace
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import stat
import struct
import subprocess
import sys
import tarfile
import threading
import time
import uuid
from urllib.parse import quote, urlencode, urlsplit
import xml.etree.ElementTree as ET

import rust_runtime_test as runtime

ROOT = runtime.ROOT
PKG = ROOT / "crates/samples/native-replicator-oracle/pkg"
BUNDLE_VERSION = 2
BUNDLE_PROFILE = "linux-x86_64-sf-11.8.121.4-volatile-fube-data-v1-native-classic-v1"
MAX_RESPONSE = 4 * 1024 * 1024
ENGINES = ("rust-runtime-app", "runtime_observer", "native-replicator-oracle")
CLASSIFICATIONS = {
    "rust-runtime-app": "native-free",
    "runtime_observer": "native-free",
    "native-replicator-oracle": "native-classic-dynamic",
}
NODE_ENGINES = {
    "N0010": "native-classic-v1", "N0020": "rust", "N0030": "native-classic-v1",
    "N0040": "rust", "N0050": "rust",
}
MIXED_NODES = ("N0010", "N0020", "N0030")
COMMON_SCENARIOS = (
    "deployment_identity", "writes_100", "identified_retry", "planned_move",
    "replacement_copy", "copy_cancellation_recovery", "apply_ack_delay",
    "unavailable_quorum", "lost_reply_dedup", "abrupt_primary_loss",
    "per_unit_close", "stale_wire_receipts", "authoritative_data_loss",
    "ledger_capacity_backpressure",
)
MIXED_SCENARIOS = (
    "rust_primary_native_secondary", "native_primary_rust_secondary",
    "native_interrupted_copy_epoch",
)


def require(condition, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def expected_files() -> dict[str, int]:
    files = {f"bin/{name}": 0o755 for name in ENGINES}
    files.update({f"launchers/launch-{name}.py": 0o755 for name in ("rust", "mixed")})
    files.update({f"packages/{profile}/{name}": 0o644
                  for profile in ("rust", "mixed") for name in runtime.PACKAGE_FILES})
    files.update({f"legacy/{name}": 0o755 for name in ("rust-runtime-app", "runtime_observer")})
    files.update({f"legacy/package-v{version}/{name}": 0o644
                  for version in (1, 2) for name in runtime.PACKAGE_FILES})
    files["legacy/build-info.json"] = 0o644
    return files


def strict_json(text: str):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate JSON metadata key")
            result[key] = value
        return result
    return json.loads(text, object_pairs_hook=unique)


def native_free_graph(graph: str) -> None:
    runtime.validate_dependency_graph(graph)
    require("fabricclient" not in graph.lower(), "native FabricClient dependency")


def validate_graphs(graphs: dict) -> None:
    require(isinstance(graphs, dict) and set(graphs) == set(ENGINES), "dependency graph inventory")
    for name in ENGINES[:2]:
        native_free_graph(graphs[name])
    graph = graphs["native-replicator-oracle"]
    require(isinstance(graph, str) and all(token in graph.lower()
            for token in ("native-replicator-oracle", "mssf-core", "mssf-com")),
            "native oracle must explicitly contain published native bindings")


def elf_identity(path: Path, classification: str) -> dict:
    with path.open("rb") as file:
        header = file.read(64)
    require(len(header) == 64 and header[:6] == b"\x7fELF\x02\x01"
            and struct.unpack_from("<H", header, 18)[0] == 62, "expected Linux x86-64 ELF")
    dynamic = runtime.run("readelf", "-d", str(path))
    require(len(dynamic) <= 131072, "ELF dynamic evidence bound")
    if classification == "native-free":
        require(not any(token in dynamic.lower() for token in
                        ("fabricruntime", "fabriccommon", "fabricclient")), "native-free ELF imports native SF")
    return {"class": "ELF64", "machine": "EM_X86_64", "classification": classification,
            "dynamic": dynamic}


@dataclass(frozen=True)
class Bundle:
    directory: Path
    source_revision: str
    manifest_sha256: str
    metadata: dict
    legacy: runtime.Binaries

    def artifact(self, name: str, copied: Path | None = None) -> Path:
        path = self.directory / name if copied is None else copied
        expected = self.metadata["artifacts"][name]
        require(not path.is_symlink() and path.is_file() and stat.S_ISREG(path.stat().st_mode)
                and stat.S_IMODE(path.stat().st_mode) == expected["mode"]
                and runtime.digest(path) == expected["sha256"], f"verified bundle artifact drift: {name}")
        return path

    @property
    def observer(self) -> Path:
        return self.artifact("bin/runtime_observer")

    def binary(self, kind: str) -> Path:
        require(kind in {"rust", "native-classic-v1"}, "unknown engine classification")
        return self.artifact("bin/" + (
            "rust-runtime-app" if kind == "rust" else "native-replicator-oracle"))


def build_bundle(directory: Path) -> None:
    revision = runtime.clean_source_revision()
    require(not directory.exists() and not directory.is_symlink(), "bundle destination must be NEW")
    directory.mkdir(parents=True, exist_ok=False)
    runtime.build_bundle(directory / "legacy")
    runtime.run("cargo", "build", "--locked", "-p", "native-replicator-oracle")
    sources = {
        "rust-runtime-app": directory / "legacy/rust-runtime-app",
        "runtime_observer": directory / "legacy/runtime_observer",
        "native-replicator-oracle": ROOT / "target/debug/native-replicator-oracle",
    }
    graphs = {name: runtime.run("cargo", "tree", "--locked", "-p",
                               "mssf-client" if name == "runtime_observer" else name)
              for name in ENGINES}
    validate_graphs(graphs)
    for name, source in sources.items():
        target = directory / "bin" / name
        target.parent.mkdir(exist_ok=True)
        shutil.copyfile(source, target)
        target.chmod(0o755)
    for profile in ("rust", "mixed"):
        launcher = directory / f"launchers/launch-{profile}.py"
        launcher.parent.mkdir(exist_ok=True)
        if profile == "mixed":
            launcher.write_text(render_mixed_launcher(directory), encoding="utf-8")
        else:
            shutil.copyfile(PKG / launcher.name, launcher)
        launcher.chmod(0o755)
        for name in runtime.PACKAGE_FILES:
            target = directory / f"packages/{profile}" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(PKG / profile / name, target)
            target.chmod(0o644)
    for name, mode in expected_files().items():
        (directory / name).chmod(mode)
    elf = {name: elf_identity(directory / "bin" / name, CLASSIFICATIONS[name]) for name in ENGINES}
    require(runtime.clean_source_revision() == revision, "source changed during immutable data build")
    metadata = {
        "version": BUNDLE_VERSION, "profile": BUNDLE_PROFILE, "source_revision": revision,
        "dependency_graphs": graphs, "elf": elf,
        "artifacts": {name: {"sha256": runtime.digest(directory / name), "mode": mode}
                      for name, mode in expected_files().items()},
    }
    manifest = directory / "build-info.json"
    with manifest.open("x", encoding="utf-8") as file:
        json.dump(metadata, file, indent=2)
    manifest.chmod(0o644)
    # Includes exact inventories and the independently strict nested v3 loader.
    try:
        load_bundle(directory)
        require(runtime.clean_source_revision() == revision, "source changed before immutable data build finished")
    except Exception:
        manifest.unlink()
        raise


def render_mixed_launcher(directory: Path) -> str:
    source = (PKG / "launch-mixed.py").read_text(encoding="utf-8")
    for name in ("rust-runtime-app", "native-replicator-oracle"):
        marker = "@" + name + ":sha256@"
        require(source.count(marker) == 1, "mixed launcher prebuilt hash marker drift")
        source = source.replace(marker, runtime.digest(directory / "bin" / name))
    return source


def load_bundle(directory: Path) -> Bundle:
    require(not directory.is_symlink(), "bundle directory must not be a symlink")
    directory = directory.resolve(strict=True)
    manifest = directory / "build-info.json"
    require(not manifest.is_symlink() and manifest.is_file()
            and stat.S_ISREG(manifest.stat().st_mode)
            and stat.S_IMODE(manifest.stat().st_mode) == 0o644
            and manifest.stat().st_size <= 2 * 1024 * 1024, "bounded regular bundle metadata/mode")
    metadata = strict_json(manifest.read_text(encoding="utf-8"))
    require(isinstance(metadata, dict) and set(metadata) == {
        "version", "profile", "source_revision", "dependency_graphs", "elf", "artifacts"
    }, "data bundle exact schema")
    require(type(metadata["version"]) is int and metadata["version"] == BUNDLE_VERSION
            and metadata["profile"] == BUNDLE_PROFILE
            and isinstance(metadata["source_revision"], str)
            and re.fullmatch(r"[a-f0-9]{40}", metadata["source_revision"]), "unsupported data bundle/source")
    validate_graphs(metadata["dependency_graphs"])
    expected = expected_files()
    entries = []
    for entry in directory.rglob("*"):
        require(len(entries) < 160, "bundle inventory bound")
        entries.append(entry)
    require(all(not entry.is_symlink() and (entry.is_file() or entry.is_dir()) for entry in entries),
            "bundle contains nonregular/symlinked entries")
    directories = {parent.as_posix() for name in expected for parent in Path(name).parents
                   if parent != Path(".")}
    require({entry.relative_to(directory).as_posix() for entry in entries if entry.is_file()}
            == set(expected) | {"build-info.json"}
            and {entry.relative_to(directory).as_posix() for entry in entries if entry.is_dir()}
            == directories, "bundle exact inventory mismatch")
    artifacts = metadata["artifacts"]
    require(isinstance(artifacts, dict) and set(artifacts) == set(expected), "artifact exact inventory")
    for name, mode in expected.items():
        entry, item = directory / name, artifacts[name]
        require(isinstance(item, dict) and set(item) == {"sha256", "mode"}
                and type(item["mode"]) is int and item["mode"] == mode
                and stat.S_IMODE(entry.stat().st_mode) == mode, f"artifact regular file mode: {name}")
        require(isinstance(item["sha256"], str) and re.fullmatch(r"[a-f0-9]{64}", item["sha256"])
                and runtime.digest(entry) == item["sha256"], f"artifact hash mismatch: {name}")
    for profile in ("rust", "mixed"):
        template = directory / "packages" / profile / "ApplicationManifest.xml"
        require(template.stat().st_size <= 65536, "application ownership template byte bound")
        root = ET.fromstring(template.read_text())
        require(root.tag == f"{{{runtime.NS}}}ApplicationManifest", "application ownership template schema")
        for parameter in ("FubeDataTypeCapability", "FubeDataCreationCapability"):
            matches = root.findall(f"{{{runtime.NS}}}Parameters/{{{runtime.NS}}}Parameter[@Name='{parameter}']")
            require(len(matches) == 1 and matches[0].get("DefaultValue") == "",
                    "bundle requires empty, unambiguous ownership parameter templates before allocation")
    elf = metadata["elf"]
    require(isinstance(elf, dict) and set(elf) == set(ENGINES), "ELF evidence inventory")
    for name in ENGINES:
        require(isinstance(elf[name], dict) and set(elf[name]) == {
            "class", "machine", "classification", "dynamic"
        } and elf[name] == elf_identity(directory / "bin" / name, CLASSIFICATIONS[name]),
            f"ELF classification/identity mismatch: {name}")
    legacy = runtime.load_bundle(directory / "legacy")
    for version in (1, 2):
        template = directory / "legacy" / f"package-v{version}" / "ApplicationManifest.xml"
        require(template.stat().st_size <= 65536, "legacy ownership template byte bound")
        root = ET.fromstring(template.read_text())
        require(root.tag == f"{{{runtime.NS}}}ApplicationManifest"
                and len(root.findall(f"{{{runtime.NS}}}Parameters")) <= 1,
                "legacy ownership template schema/parameter container")
        for parameter in ("FubeDataTypeCapability", "FubeDataCreationCapability"):
            matches = root.findall(f"{{{runtime.NS}}}Parameters/{{{runtime.NS}}}Parameter[@Name='{parameter}']")
            require(len(matches) <= 1 and (not matches or matches[0].get("DefaultValue") == ""),
                    "legacy bundle ownership parameters must be absent or empty before allocation")
    require(legacy.source_revision == metadata["source_revision"], "legacy/data source mismatch")
    require(runtime.digest(legacy.app) == artifacts["bin/rust-runtime-app"]["sha256"]
            and runtime.digest(legacy.observer) == artifacts["bin/runtime_observer"]["sha256"],
            "legacy/data executable identity mismatch")
    return Bundle(directory, metadata["source_revision"], runtime.digest(manifest), metadata, legacy)


def package(bundle: Bundle, run_id: str, mixed: bool) -> tuple[Path, str, str]:
    require(re.fullmatch(r"[a-f0-9]{32}", run_id), "invalid minted run UUID")
    profile = "mixed" if mixed else "rust"
    destination = ROOT / "build/fube-data" / run_id
    shutil.copytree(bundle.directory / "packages" / profile, destination)
    entries = list(destination.rglob("*"))
    directories = {parent.as_posix() for name in runtime.PACKAGE_FILES for parent in Path(name).parents
                   if parent != Path(".")}
    require(len(entries) <= 16 and all(not entry.is_symlink() and (entry.is_file() or entry.is_dir())
            for entry in entries) and {entry.relative_to(destination).as_posix()
            for entry in entries if entry.is_file()} == set(runtime.PACKAGE_FILES)
            and {entry.relative_to(destination).as_posix() for entry in entries if entry.is_dir()} == directories,
            "copied package template inventory drift")
    for name in runtime.PACKAGE_FILES:
        bundle.artifact(f"packages/{profile}/{name}", destination / name)
    code = destination / "RustRuntimeService/Code"
    code.mkdir()
    for kind in (("rust", "native-classic-v1") if mixed else ("rust",)):
        binary = bundle.binary(kind)
        shutil.copy2(binary, code / binary.name)
        bundle.artifact("bin/" + binary.name, code / binary.name)
    launcher = bundle.artifact(f"launchers/launch-{profile}.py")
    shutil.copy2(launcher, code / launcher.name)
    bundle.artifact("launchers/" + launcher.name, code / launcher.name)
    path = destination / "ApplicationManifest.xml"
    manifest = ET.parse(path)
    name = f"FubeData{run_id}Type"
    manifest.getroot().set("ApplicationTypeName", name)
    marker = manifest.find(f"{{{runtime.NS}}}Parameters/{{{runtime.NS}}}Parameter[@Name='FubeDataTypeCapability']")
    require(marker is not None, "missing type creation capability parameter")
    marker.set("DefaultValue", secrets.token_hex(32))
    manifest.write(path, encoding="utf-8", xml_declaration=True)
    return destination, name, manifest.getroot().attrib["ApplicationTypeVersion"]


def service_description(application: str, service: str, nodes: tuple[str, ...],
                        target: int = 3, minimum: int = 2) -> dict:
    require(service.startswith(application + "/") and all(node in NODE_ENGINES for node in nodes)
            and bool(nodes) and 1 <= minimum <= target <= len(nodes), "owned service placement")
    return {
        "ServiceKind": "Stateful", "ApplicationName": application, "ServiceName": service,
        "ServiceTypeName": "RustStatefulRuntimeType",
        # Azure SDK StatefulServiceDescription serializes byte[] as JSON integers.
        "InitializationData": list(b"fube-data-v1"),
        "PartitionDescription": {"PartitionScheme": "Singleton"},
        "HasPersistedState": False, "TargetReplicaSetSize": target, "MinReplicaSetSize": minimum,
        "ServicePackageActivationMode": "SharedProcess",
        "PlacementConstraints": " || ".join(f"NodeName == {node}" for node in nodes),
    }


POST_SERVICE = r"""
import json,sys,urllib.request
body=sys.argv[2].encode("utf-8")
request=urllib.request.Request("http://127.0.0.1:19080"+sys.argv[1],data=body,
    headers={"Content-Type":"application/json"},method="POST")
with urllib.request.urlopen(request,timeout=30) as response:
    if response.status!=202: raise RuntimeError("service create expected HTTP 202")
    data=response.read(65537)
    if len(data)>65536: raise RuntimeError("management response bound")
    print(json.dumps({"http_status":response.status,"body":data.decode()}))
"""


def create_data_service(allocation, application: str, service: str, nodes: tuple[str, ...],
                        target: int = 3, minimum: int = 2) -> dict:
    sid = application.removeprefix("fabric:/").replace("/", "~")
    description = service_description(application, service, nodes, target, minimum)
    result = json.loads(allocation.execute("python3", "-c", POST_SERVICE,
        f"/Applications/{quote(sid, safe='')}/$/GetServices/$/Create?api-version=6.0",
        json.dumps(description), timeout=40))
    return {"request": description, "response": result}


NATIVE_MANAGEMENT = r"""
import json,sys,urllib.error,urllib.request
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):
        raise RuntimeError("native management redirect refused")
method,path=sys.argv[1:]
request=urllib.request.Request("http://127.0.0.1:19080"+path,
    data=b"" if method=="POST" else None,method=method)
try:
    response=urllib.request.build_opener(NoRedirect).open(request,timeout=10)
except urllib.error.HTTPError as error:
    response=error
with response:
    body=response.read(4194305)
    if len(body)>4194304: raise RuntimeError("native management response bound")
    try:
        value=json.loads(body) if body else None
    except (ValueError,UnicodeError):
        value={"non_json_body":body[:4096].decode("utf-8",errors="replace")}
    print(json.dumps({"http_status":response.code,"body":value}))
"""


def native_management(allocation, method: str, path: str) -> dict:
    require(method in {"GET", "POST"} and path.startswith("/") and not path.startswith("//")
            and not urlsplit(path).netloc and not urlsplit(path).fragment,
            "bounded local native management request")
    reply = strict_json(allocation.execute("python3", "-c", NATIVE_MANAGEMENT, method, path, timeout=15))
    require(type(reply) is dict and set(reply) == {"http_status", "body"}
            and type(reply["http_status"]) is int, "native management response schema")
    return reply


def native_success(reply: dict, status: int, description: str):
    require(reply["http_status"] == status,
            f"required native {description} API failed: HTTP {reply['http_status']}: {reply['body']}")
    return reply["body"]


def native_uuid(value: str) -> str:
    require(type(value) is str, "native UUID must be a string")
    parsed = uuid.UUID(value)
    require(parsed.int != 0 and str(parsed) == value.lower(), "canonical nonzero native UUID required")
    return str(parsed)


def service_path(service: str) -> str:
    require(service.startswith("fabric:/") and len(service) > len("fabric:/"), "native service URI required")
    return "/Services/" + quote(service.removeprefix("fabric:/").replace("/", "~"), safe="")


def fas_path(service: str, partition: str, operation_id: str, action: str) -> str:
    require(action in {"StartDataLoss", "GetDataLossProgress", "Cancel"}, "unsupported native FAS action")
    parameters = {"api-version": "6.0", "OperationId": native_uuid(operation_id)}
    if action == "Cancel":
        parameters["Force"] = "false"
        prefix = "/Faults/$/Cancel"
    else:
        prefix = "/Faults" + service_path(service) + "/$/GetPartitions/" + native_uuid(partition) + "/$/" + action
        if action == "StartDataLoss":
            parameters["DataLossMode"] = "PartialDataLoss"
    return prefix + "?" + urlencode(parameters)


FAS_TERMINAL = {"Completed", "Faulted", "Cancelled", "ForceCancelled"}


def fas_state(body: dict, operation: dict, *, completed_required: bool = True) -> str:
    # Native httpgateway/InvokeDataLossProgress.h and management/FaultAnalysisService/
    # InvokeDataLossResult.h, SelectedPartition.h, TestCommandProgressState.h at
    # service-fabric 51a7703a1f83749a9234d3ceb4296d8622e42cca define this JSON.
    require(type(body) is dict and body.get("State") in FAS_TERMINAL | {"Running", "RollingBack"},
            "required native FAS progress schema/state unavailable")
    state = body["State"]
    if state == "Completed":
        result = body.get("InvokeDataLossResult")
        require(type(result) is dict and type(result.get("ErrorCode")) is int and result["ErrorCode"] == 0,
                "native FAS completed with a missing/nonzero result ErrorCode")
        selected = result.get("SelectedPartition")
        require(type(selected) is dict and selected.get("ServiceName") == operation["service"]
                and native_uuid(selected.get("PartitionId")) == operation["partition"],
                "native FAS result selected a different owned service/partition")
    if completed_required:
        require(state not in FAS_TERMINAL - {"Completed"}, f"required native FAS operation ended {state}: {body}")
    require(state != "ForceCancelled", "native FAS force cancellation cannot establish transport cleanup")
    return state


def poll_fas(allocation, operation: dict, *, timeout: float = 240, cancelling: bool = False) -> dict:
    require(0 < timeout <= 300, "bounded native FAS polling timeout")
    deadline = time.monotonic() + timeout
    samples = operation.setdefault("progress", [])
    for _ in range(301):
        if time.monotonic() >= deadline:
            break
        reply = native_management(allocation, "GET", fas_path(
            operation["service"], operation["partition"], operation["operation_id"], "GetDataLossProgress"))
        body = native_success(reply, 200, "GetDataLossProgress")
        samples.append({"at": time.time(), "reply": reply})
        require(len(samples) <= 610, "bounded native FAS progress inventory")
        state = fas_state(body, operation, completed_required=False)
        operation["state"] = state
        if not cancelling:
            fas_state(body, operation)
        if state in FAS_TERMINAL:
            operation["native_cleanup_terminal"] = True
            return body
        time.sleep(min(1, max(0, deadline - time.monotonic())))
    raise RuntimeError("required native FAS operation did not reach a cleanup-terminal state within bounded polling")


def cancel_fas(allocation, operation: dict) -> None:
    if operation.get("native_cleanup_terminal"):
        return
    # The UUID was minted and recorded before StartDataLoss; cancellation never
    # targets a command discovered by name or another user's command inventory.
    try:
        reply = native_management(allocation, "GET", fas_path(
            operation["service"], operation["partition"], operation["operation_id"], "GetDataLossProgress"))
        operation["cleanup_query"] = reply
        if reply["http_status"] == 404 and isinstance(reply["body"], dict) and (
                reply["body"].get("Error", {}).get("Code") == "FABRIC_E_TEST_COMMAND_OPERATION_ID_NOT_FOUND"):
            require(operation.get("start_http_status") != 202, "accepted native FAS operation disappeared before cleanup")
            operation.update(state="Absent", native_cleanup_terminal=True)
            return
        body = native_success(reply, 200, "GetDataLossProgress cleanup")
        state = fas_state(body, operation, completed_required=False)
        operation["state"] = state
        if state in FAS_TERMINAL:
            operation["native_cleanup_terminal"] = True
            return
    except Exception as error:
        operation["cleanup_query_error"] = f"{type(error).__name__}: {error}"
    operation["cancel_requested"] = True
    reply = native_management(allocation, "POST", fas_path(
        operation["service"], operation["partition"], operation["operation_id"], "Cancel"))
    operation["cancel_reply"] = reply
    # Completion can race cancellation. Only a terminal progress observation,
    # never the cancellation response alone, establishes cleanup.
    if reply["http_status"] != 200:
        operation["cancel_error"] = f"required native CancelTestCommand API failed: {reply}"
    poll_fas(allocation, operation, timeout=90, cancelling=True)


def request(address: str, operation: str | None = None, parameters: dict | None = None,
            timeout: float = 9) -> tuple[int, dict]:
    url = urlsplit(address)
    require(url.scheme == "http" and url.hostname and url.port and not url.username
            and not url.password and not url.query and not url.fragment
            and url.path.endswith("/service"), "owned data HTTP endpoint required (never Fabric TCP)")
    path = url.path if operation is None else url.path.rsplit("/", 1)[0] + "/" + operation
    if parameters:
        path += "?" + urlencode(parameters)
    connection = http.client.HTTPConnection(url.hostname, url.port, timeout=timeout)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        body = response.read(MAX_RESPONSE + 1)
        require(len(body) <= MAX_RESPONSE, "bounded data HTTP response")
        value = strict_json(body.decode("utf-8"))
        require(isinstance(value, dict), "data response must be an object")
        return response.status, value
    finally:
        connection.close()


def native_snapshot(bundle: Bundle, service: str) -> dict:
    text = runtime.run(str(bundle.observer), "127.0.0.1:19000", service, "--stateful", timeout=60)
    require(len(text) <= MAX_RESPONSE and len(text.splitlines()) == 1, "bounded native data observation")
    value = strict_json(text)
    require(isinstance(value, dict) and value.get("observer") == "mssf-client"
            and value.get("service") == service and value.get("partition"), "native observer service identity")
    return value


def identity(replica: dict) -> tuple:
    rpc = replica["rpc"]
    return replica["node"], replica["replica"], rpc["incarnation"], rpc["engine_incarnation"], rpc["host"], rpc["pid"]


def assert_data(value: dict, service: str, mixed: bool, target: int = 3, minimum: int = 2,
                *, require_full: bool = True) -> dict:
    require(value.get("observer") == "mssf-client" and value.get("service") == service,
            "native data service identity")
    native = value["native_partition"]
    require(native["target"] == target and native["minimum"] == minimum and native["status"] == "Ready",
            "native partition is not ready at its actual requested size")
    ready = [replica for replica in value["replicas"] if replica["status"] == "Ready"]
    require((len(ready) == target if require_full else minimum <= len(ready) <= target)
            and len({r["node"] for r in ready}) == len(ready)
            and len({r["replica"] for r in ready}) == len(ready)
            and sum(r["role"] == "Primary" for r in ready) == 1, "native role/placement cardinality")
    kinds = set()
    for replica in ready:
        rpc, deployed = replica["rpc"], replica["deployed"]
        require(replica["role"] in {"Primary", "ActiveSecondary"}, "inactive ready data replica")
        role = "Primary" if replica["role"] == "Primary" else "Secondary"
        kind = NODE_ENGINES.get(replica["node"]) if mixed else "rust"
        require(kind is not None and (not mixed or replica["node"] in MIXED_NODES), "unexpected engine placement")
        kinds.add(kind)
        service_role = replica["role"] if kind == "native-classic-v1" else role
        require(rpc.get("profile") == "fube-data-v1" and rpc.get("engine_kind") == kind
                and rpc["partition"] == value["partition"] and rpc["replica"] == replica["replica"]
                and rpc["node"] == replica["node"] and rpc["pid"] == deployed["host_process_id"]
                and rpc["service_address"] == replica["address"]
                and rpc["role"] == service_role and rpc["current_role"] == service_role
                and deployed["service_type_name"] == "RustStatefulRuntimeType"
                and rpc.get("host") and rpc.get("engine_incarnation"), "native deployment/data HTTP identity mismatch")
        if "service" in rpc:
            require(rpc["service"] == service, "HTTP service mismatch")
        epoch = rpc["native_epoch"]
        if kind == "native-classic-v1" and role == "Secondary":
            # This is the last PUBLIC provider callback, not the native
            # replicator's current epoch. Initial same-epoch replication need
            # never call UpdateEpoch; later callbacks may lag FM until a barrier.
            # Preserve null/older observations; never invent a current epoch.
            if epoch is not None:
                require(isinstance(epoch, dict) and type(epoch.get("data_loss")) is int
                        and type(epoch.get("configuration")) is int
                        and epoch["data_loss"] >= 0 and epoch["configuration"] >= 0
                        and (epoch["data_loss"], epoch["configuration"] & ~0xFFFFFFFF)
                        <= (native["epoch"]["data_loss"], native["epoch"]["configuration"]),
                        "native provider callback epoch is invalid or ahead of authority")
        else:
            require(isinstance(epoch, dict) and epoch["data_loss"] == native["epoch"]["data_loss"]
                    and epoch["configuration"] & ~0xFFFFFFFF == native["epoch"]["configuration"],
                    "native projected/full epoch mismatch")
        require(not rpc["state"]["copy_in_progress"] and rpc["engine"]["copy_done"]
                and not rpc["engine"]["closed"] and not rpc["engine"]["fault"] and not rpc["pump_fault"],
                "data provider/engine not ready or faulted")
        if kind == "rust":
            require(rpc["replicator_role"] == role and isinstance(rpc["replication_address"], str)
                    and not rpc["replication_address"].startswith("http")
                    and rpc["replication_address"] != replica["address"], "Rust data/control separation")
        else:
            # Native EndOpen belongs to RA. Null is honest; opening twice is not.
            require(rpc["replicator_role"] is None and rpc["replication_address"] is None,
                    "native fixture invented a public EndOpen observation")
            settings, requested = rpc["native_settings"], rpc["requested_native_settings"]
            require(isinstance(settings, dict) and not rpc["native_settings_error"]
                    and settings["require_service_ack"] in (True, 1)
                    and requested["require_service_ack"] is True
                    and type(requested["require_service_ack_flag"]) is int
                    and requested["require_service_ack_flag"] > 0
                    and settings["flags"] & requested["require_service_ack_flag"]
                        == requested["require_service_ack_flag"], "effective native RequireServiceAck flag missing")
        if role == "Primary":
            require(replica["service_http_status"] == 200 and rpc["read_status"] == "Granted"
                    and rpc["write_status"] == "Granted", "native primary access is not granted")
        else:
            require(replica["service_http_status"] == 503, "secondary serves authoritative reads")
    require(not require_full or not mixed or target != 3 or kinds == {"rust", "native-classic-v1"},
            "actual partition is not mixed")
    primary = value["primary"]
    placed = next(r for r in ready if r["role"] == "Primary")
    require(primary["resolver"] == "mssf-client"
            and primary["endpoint"] == placed["address"] and primary["native_pid"] == placed["rpc"]["pid"]
            and all(primary["rpc"][key] == placed["rpc"][key]
                    for key in ("partition", "replica", "incarnation", "node", "host", "pid")),
            "native primary resolver/deployed code/data identity mismatch")
    return value


class Ledger:
    """Keep accepted identities/sequences even when their HTTP result is lost."""
    def __init__(self):
        self.records: dict[str, dict] = {}

    def mutation(self, request_id: str) -> dict:
        if request_id not in self.records:
            self.records[request_id] = {
                "id": request_id, "key": "key-" + request_id, "value": "value-" + request_id,
                "sequence": None, "committed": False, "attempts": [],
            }
        return self.records[request_id]

    def reply(self, record: dict, status: int, reply: dict) -> str:
        require(reply.get("request_id") == record["id"], "write response ID mismatch")
        outcome = reply.get("outcome")
        require((status, outcome) in {(200, "committed"), (202, "ambiguous"), (503, "rejected")},
                "write HTTP status/outcome mismatch")
        sequence = reply.get("sequence")
        require((outcome == "rejected" and sequence is None) or
                (outcome != "rejected" and type(sequence) is int and sequence > 0),
                "accepted nonzero write sequence/rejection mismatch")
        if sequence is not None:
            require(record["sequence"] in (None, sequence), "identified retry changed its accepted sequence")
            require(not any(other["id"] != record["id"] and other["sequence"] == sequence
                            for other in self.records.values()), "different IDs share a sequence")
            record["sequence"] = sequence
        record["committed"] |= outcome == "committed"
        record["attempts"].append({"at": time.monotonic(), "http_status": status, "reply": reply})
        return outcome

    def compare(self, value: dict, *, allow_extra: bool = False) -> dict:
        snapshots = [replica["rpc"]["state"] for replica in value["replicas"] if replica["status"] == "Ready"]
        require(bool(snapshots), "no independently observed providers")
        results = snapshots[0]["results"]
        for record in self.records.values():
            observed = results.get(record["id"])
            if record["sequence"] is not None:
                require(observed == record["sequence"], "accepted/acknowledged ledger ID missing or changed")
            elif observed is not None:
                require(type(observed) is int and observed > 0, "observed accepted sequence")
                record["sequence"] = observed
        ordered = sorted((r for r in self.records.values() if r["sequence"] is not None),
                         key=lambda r: r["sequence"])
        expected_results = {r["id"]: r["sequence"] for r in ordered}
        expected_values = {r["key"]: r["value"] for r in ordered}
        require(len({r["sequence"] for r in ordered}) == len(ordered), "nonunique accepted sequences")
        for snapshot in snapshots:
            require(not snapshot["copy_in_progress"] and snapshot["results"] == results
                    and snapshot["values"] == snapshots[0]["values"]
                    and snapshot["last"] == snapshots[0]["last"], "provider maps/ledgers/progress differ")
            if not allow_extra:
                require(snapshot["results"] == expected_results and snapshot["values"] == expected_values,
                        "independent provider map/ledger differs from identified workload")
        return {"successful": sum(r["committed"] for r in ordered), "accepted": len(ordered),
                "last": snapshots[0]["last"], "results": expected_results, "values": expected_values}


class Client:
    def __init__(self, bundle: Bundle, service: str, mixed: bool, ledger: Ledger,
                 target: int = 3, minimum: int = 2):
        self.bundle, self.service, self.mixed, self.ledger = bundle, service, mixed, ledger
        self.target, self.minimum = target, minimum
        self.endpoint: str | None = None

    def refresh(self) -> dict:
        observed = assert_data(native_snapshot(self.bundle, self.service), self.service, self.mixed,
                               self.target, self.minimum, require_full=False)
        self.endpoint = observed["primary"]["endpoint"]
        return observed

    def write(self, request_id: str, timeout: float = 40, stop: threading.Event | None = None) -> dict:
        record = self.ledger.mutation(request_id)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            require(stop is None or not stop.is_set(), "owned write workload cancelled")
            if self.endpoint is None:
                try:
                    self.refresh()
                except (RuntimeError, subprocess.TimeoutExpired, OSError) as error:
                    record["attempts"].append({"at":time.monotonic(),"outcome":"observation_unavailable",
                                               "error":str(error)})
                    time.sleep(.1)
                    continue
            try:
                status, reply = request(self.endpoint, "write",
                                        {key: record[key] for key in ("id", "key", "value")})
                outcome = self.ledger.reply(record, status, reply)
                if outcome == "committed":
                    return record
                if outcome == "rejected" and record["sequence"] is None:
                    # Only definite pre-admission rejection allows a fresh attempt.
                    self.endpoint = None
                else:
                    # An accepted operation is never undone. Same ID/sequence only.
                    time.sleep(0.1)
                    self.endpoint = None
            except (OSError, http.client.HTTPException) as error:
                record["attempts"].append({"at": time.monotonic(), "outcome": "ambiguous_transport",
                                           "error": str(error)})
                self.endpoint = None
        last = record["attempts"][-1] if record["attempts"] else None
        raise RuntimeError(f"identified write remains unresolved: {request_id}, "
                           f"sequence={record['sequence']}, last_attempt={last}")


DATA_PROCESS = r"""
import hashlib,json,os,select,signal,sys
from pathlib import Path
expected=json.loads(sys.argv[1]);action=sys.argv[2];pid=expected["native_pid"]
def require(value,message):
    if not value: raise RuntimeError(message)
def digest(path):
    value=hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda:file.read(1048576),b""):value.update(chunk)
    return value.hexdigest()
require(type(pid) is int and pid>1,"invalid native-deployed PID")
fd=os.pidfd_open(pid)
try:
    root=Path("/proc")/str(pid);exe=os.readlink(root/"exe")
    launcher=Path(expected["native_entry_point"])
    require(launcher.is_absolute() and launcher.name==expected["launcher_name"],"native launcher identity")
    require(not launcher.is_symlink() and launcher.is_file() and digest(launcher)==expected["launcher_sha256"],
            "deployed launcher SHA differs from bundle")
    require(exe==str(launcher.parent/expected["engine_name"]),"post-exec native executable identity")
    require(digest(root/"exe")==expected["engine_sha256"],"post-exec engine SHA differs from bundle")
    env=dict(item.split("=",1) for item in (root/"environ").read_bytes().decode().split("\0") if "=" in item)
    require(env["Fabric_ApplicationHostId"]==expected["host"],"native host identity drift")
    require(env["Fabric_ApplicationName"]==expected["application"],"application identity drift")
    require(env["Fabric_NodeName"]==expected["node"],"activated selector node mismatch")
    require(env["Fabric_ServicePackageName"]=="RustRuntimeService","service package identity drift")
    sequence=int(env["Fabric_CodePackageInstanceSeqNum"])
    require(sequence>0,"invalid activated code-package sequence")
    process_stat=(root/"stat").read_text().rsplit(") ",1)[1].split()
    start=int(process_stat[19])
    paths=sorted({row.split(maxsplit=5)[5] for row in (root/"maps").read_text().splitlines()
                  if len(row.split(maxsplit=5))==6 and row.split(maxsplit=5)[5].startswith("/")})
    libraries={name:any(name in path.lower() for path in paths)
               for name in ("fabricruntime","fabriccommon","fabricclient")}
    if expected["kind"]=="rust":
        require(not any(libraries.values()),"production Rust process maps native SF libraries")
    else:
        require(all(libraries.values()),
                "native oracle did not load actual native runtime/common/client")
    captured=expected.get("captured")
    if captured is not None:
        require(type(captured) is dict
                and all(field in captured for field in (
                    "pid","start_ticks","code_sequence","host","exe","launcher_sha256","executable_sha256"))
                and all(type(captured[field]) is int and captured[field]>0
                        for field in ("pid","start_ticks","code_sequence"))
                and all(type(captured[field]) is str and bool(captured[field])
                        for field in ("host","exe","launcher_sha256","executable_sha256")),
                "malformed captured native process identity")
        require(captured["start_ticks"]==start and captured["code_sequence"]==sequence
                and captured["pid"]==pid and captured["exe"]==exe
                and captured["launcher_sha256"]==expected["launcher_sha256"]
                and captured["executable_sha256"]==expected["engine_sha256"]
                and captured["host"]==env["Fabric_ApplicationHostId"],"process/code incarnation drift")
    result={"pid":pid,"exe":exe,"start_ticks":start,"code_sequence":sequence,
            "host":env["Fabric_ApplicationHostId"],"native_libraries":libraries,"loaded_files":paths,
            "classification":"native-free" if expected["kind"]=="rust" else "native-classic-dynamic",
            "launcher_sha256":digest(launcher),"executable_sha256":digest(root/"exe"),
            "activation_environment":{key:env[key] for key in (
                "Fabric_ApplicationId","Fabric_ApplicationName","Fabric_ApplicationHostId","Fabric_NodeName",
                "Fabric_ServicePackageName","Fabric_ServicePackageVersionInstance",
                "Fabric_CodePackageInstanceSeqNum","Fabric_ServicePackageInstanceSeqNum") if key in env},
            "action":action}
    if action!="inspect":
        require(captured is not None,"mutation requires a previously captured native process identity")
        require(action in ("kill","pause","resume"),"unknown pidfd action")
        signal.pidfd_send_signal(fd,{"kill":signal.SIGKILL,"pause":signal.SIGSTOP,"resume":signal.SIGCONT}[action])
        if action=="kill":
            poll=select.poll();poll.register(fd,select.POLLIN)
            require(bool(poll.poll(3000)),"verified killed process did not exit")
            result["old_process_exit_confirmed"]=True
        elif action=="pause":
            import time
            deadline=time.monotonic()+1
            while time.monotonic()<deadline and (root/"stat").read_text().rsplit(") ",1)[1].split()[0]!="T":
                time.sleep(.01)
            require((root/"stat").read_text().rsplit(") ",1)[1].split()[0]=="T","owned process not stopped")
    print(json.dumps(result))
finally:
    os.close(fd)
"""


def native_process_id(value) -> int:
    # Native REST represents its INT64 ProcessId as a decimal JSON string.
    require(type(value) is int or (type(value) is str
            and re.fullmatch(r"[1-9][0-9]{0,9}", value)), "invalid native process ID")
    number = int(value)
    require(0 < number <= 2**31 - 1, "invalid native process ID")
    return number


def process_expected(allocation, bundle: Bundle, service: str, replica: dict, mixed: bool) -> dict:
    node, rpc = replica["node"], replica["rpc"]
    sid = service.rsplit("/", 1)[0].removeprefix("fabric:/").replace("/", "~")
    query = runtime.native_read(allocation,
        f"/Nodes/{quote(node, safe='')}/$/GetApplications/{quote(sid, safe='')}/$/GetCodePackages"
        "?api-version=6.0&ServiceManifestName=RustRuntimeService&CodePackageName=Code")
    packages = query if isinstance(query, list) else query["Items"]
    matches = [item for item in packages if item["Name"] == "Code"
               and item["ServiceManifestName"] == "RustRuntimeService"
               and native_process_id(item["MainEntryPoint"]["ProcessId"]) == rpc["pid"]]
    require(len(matches) == 1 and replica["deployed"]["host_process_id"] == rpc["pid"],
            "independent native code package/PID identity mismatch")
    package_info = matches[0]
    kind = NODE_ENGINES[node] if mixed else "rust"
    engine = bundle.binary(kind)
    launcher_name = "launch-mixed.py" if mixed else engine.name
    return {
        "native_pid": rpc["pid"], "native_entry_point": package_info["MainEntryPoint"]["EntryPointLocation"],
        "native_entry_point_instance": package_info["MainEntryPoint"]["InstanceId"],
        "native_code_version": package_info["Version"], "native_query": package_info,
        "host": rpc["host"], "node": node, "application": service.rsplit("/", 1)[0], "kind": kind,
        "engine_name": engine.name, "engine_sha256": bundle.metadata["artifacts"]["bin/" + engine.name]["sha256"],
        "launcher_name": launcher_name,
        "launcher_sha256": bundle.metadata["artifacts"][
            ("launchers/" if mixed else "bin/") + launcher_name]["sha256"],
    }


def inspect_process(allocation, expected: dict, action: str = "inspect") -> dict:
    return json.loads(allocation.execute("python3", "-c", DATA_PROCESS, json.dumps(expected), action, timeout=15))


def required_scenarios(profile: str) -> tuple[str, ...]:
    require(profile in {"rust", "mixed", "full"}, "unknown qualification profile")
    return COMMON_SCENARIOS + (MIXED_SCENARIOS if profile != "rust" else ()) + (
        ("legacy_full",) if profile == "full" else ())


def finalize(result: dict) -> dict:
    required = required_scenarios(result["profile"])
    scenarios = result["scenarios"]
    result["required_scenarios"] = list(required)
    result["status"] = "PASS" if (not result["errors"] and set(scenarios) == set(required)
        and all(isinstance(scenarios.get(name), dict) and scenarios[name].get("status") == "PASS"
                for name in required)) else "FAIL"
    result["missing_or_failed"] = [name for name in required if scenarios.get(name, {}).get("status") != "PASS"]
    return result


class MissingEvidence(RuntimeError):
    """Executed work is retained, but a required independent observation is absent."""


def stable_data(before: dict, after: dict) -> None:
    require(before["service"] == after["service"] and before["partition"] == after["partition"]
            and before["native_partition"] == after["native_partition"],
            "negative probe changed native service/partition/full epoch/configuration")
    original = {identity(replica): replica for replica in before["replicas"]}
    current = {identity(replica): replica for replica in after["replicas"]}
    require(original.keys() == current.keys(), "negative probe replaced a native/engine identity")
    for key, replica in original.items():
        other = current[key]
        require(all(replica[field] == other[field] for field in ("role", "status", "address"))
                and replica["rpc"]["native_epoch"] == other["rpc"]["native_epoch"],
                "negative probe changed a native role/route/full epoch")
        require(replica["rpc"]["state"] == other["rpc"]["state"]
                and replica["rpc"]["engine"]["progress"] == other["rpc"]["engine"]["progress"],
                "negative probe changed actual provider contents/ledger/progress or engine progress")


def wire_endpoint(value: str) -> dict:
    require(type(value) is str and len(value) <= 4096, "bounded native-selected wire endpoint")
    match = re.fullmatch(r"([^/\s]+)/([a-fA-F0-9-]{36})-([0-9]+);([a-fA-F0-9-]{36})(;[^/;\s]*)?", value)
    require(match is not None, "native-selected wire endpoint actor schema")
    address, partition, replica, engine, suffix = match.groups()
    parsed = urlsplit("//" + address)
    require(parsed.hostname and parsed.port and not parsed.username and not parsed.password,
            "native-selected TCP listener required")
    return {"address": address, "host": parsed.hostname, "partition": native_uuid(partition),
            "replica": int(replica), "engine": native_uuid(engine), "suffix": suffix}


WIRE_CAPTURE_SECONDS = 20
WIRE_CAPTURE_PACKETS = 1024
WIRE_CAPTURE_BYTES = 512 * 1024
WIRE_CAPTURE_OUTPUT = 2 * 1024 * 1024

# Read-only, test-only observer in the original container/network namespace.
# PACKET_HOST on lo observes the destination ingress clone, not PACKET_OUTGOING.
# No expected probe bytes enter this helper; they are compared independently.
WIRE_CAPTURE = r"""
import ipaddress,json,os,select,signal,socket,struct,sys,time
def require(value, message):
    if not value: raise RuntimeError(message)
deadline=float(sys.argv[2]); token=sys.argv[3]
signal.signal(signal.SIGALRM,lambda *_: sys.exit("wire observer absolute deadline"))
signal.alarm(max(1,min(20,int(deadline-time.time()+1))))
with open("/proc/self/stat") as status: start=status.read().rsplit(") ",1)[1].split()[19]
with open("/proc/self/status") as status:
    cap_eff=next(line.split()[1] for line in status if line.startswith("CapEff:"))
proof={"pid":os.getpid(),"start_time":start,"token":token}
credentials={"euid":os.geteuid(),"egid":os.getegid(),
             "cap_eff":cap_eff}
sock=None
try:
    allowed=json.loads(sys.argv[1])
    require(type(allowed) is list and 1<=len(allowed)<=64,"wire observer endpoint bound")
    pairs=[]
    for pair in allowed:
        require(type(pair) is list and len(pair)==2,"wire observer endpoint pair")
        endpoints=[]
        for endpoint in pair:
            host,port=endpoint.rsplit(":",1)
            host=str(ipaddress.IPv4Address(host))
            require(port.isdigit() and 0<int(port)<65536,"wire observer numeric IPv4 endpoint required")
            endpoints.append((host,int(port)))
        pairs.append(endpoints)
    sock=socket.socket(socket.AF_PACKET,socket.SOCK_DGRAM,socket.htons(0x0800))
    sock.bind(("lo",0)); sock.setblocking(False)
    # Reset packet statistics before readiness; loss is never a successful trace.
    sock.getsockopt(263,6,12)
    ready={**proof,"classification":"destination_kernel_ingress","interface":"lo",
           "netns_inode":os.stat("/proc/self/ns/net").st_ino,"credentials":credentials,"allowed":allowed}
    print(json.dumps(ready),flush=True)
    packets=[]; total=0; seen=0; wire_bytes=0
    while True:
        remaining=deadline-time.time()
        require(remaining>0,"wire observer deadline")
        readable,_,_=select.select([sock,sys.stdin],[],[],remaining)
        # Drain the ready socket before honouring finish on stdin.
        if sock in readable:
            for _ in range(32769):
                try: packet,address=sock.recvfrom(65536)
                except BlockingIOError: break
                seen+=1; wire_bytes+=len(packet)
                require(seen<=32768 and wire_bytes<=16777216,"wire observer packet/wire byte bound")
                if address[2]!=0: continue
                if len(packet)<40 or packet[0]>>4!=4 or packet[9]!=6: continue
                ihl=(packet[0]&15)*4; size=struct.unpack_from("!H",packet,2)[0]
                require(20<=ihl<=60 and ihl+20<=size<=len(packet),"wire observer truncated IPv4/TCP")
                require(struct.unpack_from("!H",packet,6)[0]&0x3fff==0,"wire observer fragmented IPv4")
                src=socket.inet_ntoa(packet[12:16]); dst=socket.inet_ntoa(packet[16:20])
                sport,dport,sequence=struct.unpack_from("!HHI",packet,ihl)
                tcp=(packet[ihl+12]>>4)*4
                require(tcp>=20 and ihl+tcp<=size,"wire observer truncated TCP header")
                if not any(src==a[0] and dst==b[0] and (sport==a[1] or dport==b[1]) for a,b in pairs):
                    continue
                payload=packet[ihl+tcp:size]
                if not payload: continue
                total+=len(payload)
                require(len(packets)<1024 and total<=524288,"wire observer captured payload bound")
                packets.append({"local":src+":"+str(sport),"remote":dst+":"+str(dport),
                                "sequence":sequence,"payload_hex":payload.hex()})
            else: raise RuntimeError("wire observer drain bound")
        if sys.stdin in readable:
            require(os.read(sys.stdin.fileno(),16)==b"finish\n","wire observer finish protocol")
            break
    stats=sock.getsockopt(263,6,12)
    received,dropped=struct.unpack_from("II",stats)
    require(dropped==0,"wire observer kernel packet loss")
    print(json.dumps({"helper":proof,"classification":"destination_kernel_ingress","interface":"lo",
                      "netns_inode":ready["netns_inode"],"allowed":allowed,
                      "packets":packets,"payload_bytes":total,"seen_packets":seen,
                      "wire_bytes":wire_bytes,"kernel_drops":dropped}),flush=True)
except Exception as error:
    print(json.dumps({"helper":proof,"error":str(error),"credentials":credentials}),flush=True)
    sys.exit(1)
finally:
    if sock is not None: sock.close()
"""


class WireCapture:
    def __init__(self, allocation, allowed: list, evidence: dict):
        self.allocation, self.allowed, self.evidence = allocation, allowed, evidence
        self.container_id = allocation.verify()
        self.token = secrets.token_hex(32)
        self.helper = None
        self.process = None

    def __enter__(self):
        self.deadline = time.monotonic() + WIRE_CAPTURE_SECONDS
        self.process = subprocess.Popen(
            ["docker", "exec", "-i", self.container_id, "python3", "-c", WIRE_CAPTURE,
             json.dumps(self.allowed), str(time.time() + WIRE_CAPTURE_SECONDS), self.token],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=ROOT, bufsize=0)
        try:
            import select
            line = b""
            while not line.endswith(b"\n"):
                left = self.deadline - time.monotonic()
                if left <= 0 or not select.select([self.process.stdout], [], [], left)[0]:
                    raise MissingEvidence("destination wire observer readiness deadline")
                part = os.read(self.process.stdout.fileno(), 8193 - len(line))
                if not part:
                    raise MissingEvidence("destination wire observer exited before readiness: " + line.decode(errors="replace"))
                line += part
                require(len(line) <= 8192, "wire observer readiness byte bound")
            ready = strict_json(line.decode())
            proof = ready.get("helper", ready)
            require(type(proof.get("pid")) is int and proof["pid"] > 1
                    and type(proof.get("start_time")) is str and proof["start_time"].isdigit()
                    and proof.get("token") == self.token, "wire observer original helper identity")
            self.helper = {key: proof[key] for key in ("pid", "start_time", "token")}
            self.evidence["observer_ready"] = ready
            if "error" in ready:
                raise MissingEvidence("destination wire observer unavailable using existing credentials: " + ready["error"])
            require(ready.get("classification") == "destination_kernel_ingress" and ready.get("interface") == "lo"
                    and type(ready.get("netns_inode")) is int and ready["netns_inode"] > 0
                    and ready.get("allowed") == self.allowed, "wire observer readiness scope changed")
            require(self.allocation.verify() == self.container_id, "original wire observer allocation changed")
            return self
        except BaseException:
            self.__exit__(*sys.exc_info())
            raise

    def finish(self) -> dict:
        require(self.allocation.verify() == self.container_id, "original wire observer allocation changed")
        # communicate is bounded by the helper's finite output/absolute alarm;
        # it also joins the docker client before returning evidence.
        stdout, stderr = self.process.communicate(
            b"finish\n", timeout=max(.001, self.deadline - time.monotonic()) + 2)
        require(len(stdout) <= WIRE_CAPTURE_OUTPUT and len(stderr) <= 8192, "wire observer output bound")
        if not stdout:
            raise MissingEvidence("destination wire observer produced no receipt: " + stderr.decode(errors="replace"))
        result = strict_json(stdout.decode())
        self.evidence["observer_receipt"] = result
        if self.process.returncode != 0:
            raise MissingEvidence(f"destination wire observer failed: {result}; {stderr.decode(errors='replace')}")
        require(result.get("helper") == self.helper and result.get("allowed") == self.allowed
                and result.get("netns_inode") == self.evidence["observer_ready"]["netns_inode"],
                "wire observer process/namespace/scope changed")
        return result

    def __exit__(self, exc_type, error, traceback):
        if self.process is None:
            return
        try:
            if self.helper is not None:
                require(self.allocation.verify() == self.container_id, "original wire observer allocation changed; refuse signal")
                self.allocation.execute("python3", "-c", STOP_UPLOAD_HELPER, json.dumps(self.helper), timeout=12)
                self.evidence["observer_cleanup"] = "original helper pidfd exit confirmed"
            else:
                self.evidence["observer_cleanup"] = (
                    "no original helper receipt; no PID/name adoption or signal; finite absolute alarm remains")
        except BaseException as cleanup_error:
            self.evidence["observer_cleanup_error"] = str(cleanup_error)
            if error is None:
                raise
            error.add_note("wire observer cleanup unconfirmed; absolute alarm remains: " + str(cleanup_error))
        finally:
            if self.process.poll() is None:
                self.process.kill()
            self.process.communicate(timeout=3)


def wire_capture_scope(source: dict) -> list:
    own = wire_endpoint(source["rpc"]["replication_address"])["address"]
    peers = ([peer["address"] for peer in source["rpc"]["engine"].get("peers", [])
              if peer["configured"] and not peer["building"] and not peer["cleanup_required"]]
             if source["role"] == "Primary" else [source["rpc"].get("observed_primary")])
    require(1 <= len(peers) <= 64, "wire observer requires bounded existing native-selected peers")
    return [[own, wire_endpoint(peer)["address"]] for peer in peers]


def wire_delivery(reply: dict, own: dict, selected: dict, observation: dict | None) -> dict:
    delivery = reply.get("delivery")
    if not isinstance(delivery, dict) or reply.get("admission_only") is not False:
        raise MissingEvidence("exact invalid frame socket write/flush completion missing (admission is insufficient)")
    require(delivery.get("classification") == "socket_write_all_and_flush"
            and type(delivery.get("session")) is int and delivery["session"] > 0
            and delivery.get("remote_listener") == selected["address"], "wire completion session/listener mismatch")
    frame_hex = delivery.get("frame_hex")
    require(type(frame_hex) is str and re.fullmatch(r"(?:[a-f0-9]{2}){12,16384}", frame_hex) is not None,
            "wire completion exact bounded frame bytes required")
    frame = bytes.fromhex(frame_hex)
    require(type(delivery.get("frame_bytes")) is int and delivery["frame_bytes"] == len(frame)
            and struct.unpack_from("<I", frame)[0] == len(frame), "wire completion frame length mismatch")
    message_id = delivery.get("message_id")
    require(isinstance(message_id, dict) and native_uuid(message_id.get("guid")) != str(uuid.UUID(int=0))
            and message_id.get("index") == 1
            and frame.count(uuid.UUID(message_id["guid"]).bytes_le) == 1,
            "wire completion unique encoded message identity missing")
    local, remote = (urlsplit("//" + delivery.get(key, "")) for key in ("local", "remote"))
    own_socket, peer_socket = urlsplit("//" + own["address"]), urlsplit("//" + selected["address"])
    require(local.hostname == own_socket.hostname and remote.hostname == peer_socket.hostname
            and local.port and remote.port
            and (local.port == own_socket.port or remote.port == peer_socket.port),
            "wire completion actual socket tuple is not the selected session")
    if not isinstance(observation, dict):
        raise MissingEvidence("independent destination-wire evidence missing")
    require(observation.get("classification") == "destination_kernel_ingress"
            and observation.get("interface") == "lo"
            and type(observation.get("kernel_drops")) is int and observation["kernel_drops"] == 0
            and [own["address"], selected["address"]] in observation.get("allowed", [])
            and type(observation.get("seen_packets")) is int and 0 <= observation["seen_packets"] <= 32768
            and type(observation.get("wire_bytes")) is int and 0 <= observation["wire_bytes"] <= 16777216,
            "wire evidence is not loss-free destination ingress")
    packets = observation.get("packets")
    require(type(packets) is list and len(packets) <= WIRE_CAPTURE_PACKETS,
            "wire evidence packet bound")
    payload_bytes = 0
    chunks = []
    anchor = None
    for packet in packets:
        require(type(packet) is dict and type(packet.get("payload_hex")) is str
                and len(packet["payload_hex"]) <= 131072
                and re.fullmatch(r"(?:[a-f0-9]{2})+", packet["payload_hex"]) is not None
                and type(packet.get("sequence")) is int and 0 <= packet["sequence"] < 2**32,
                "wire evidence malformed TCP payload")
        payload = bytes.fromhex(packet["payload_hex"])
        payload_bytes += len(payload)
        require(payload_bytes <= WIRE_CAPTURE_BYTES, "wire evidence payload byte bound")
        if (packet.get("local"), packet.get("remote")) != (delivery["local"], delivery["remote"]):
            continue
        if anchor is None:
            anchor = packet["sequence"]
        offset = ((packet["sequence"] - anchor + 2**31) % 2**32) - 2**31
        require(abs(offset) <= WIRE_CAPTURE_BYTES, "wire evidence TCP sequence window bound")
        chunks.append((offset, payload))
    require(observation.get("payload_bytes") == payload_bytes, "wire evidence payload accounting mismatch")
    streams = []
    for offset, payload in sorted(chunks):
        if not streams or offset > streams[-1][0] + len(streams[-1][1]):
            streams.append((offset, bytearray(payload)))
            continue
        start, stream = streams[-1]
        overlap = min(len(payload), start + len(stream) - offset)
        require(stream[offset-start:offset-start+overlap] == payload[:overlap],
                "wire evidence conflicting TCP retransmission")
        stream.extend(payload[overlap:])
    matches = [(offset + stream.find(frame), stream) for offset, stream in streams if frame in stream]
    if len(matches) != 1 or matches[0][1].count(frame) != 1:
        raise MissingEvidence("exact invalid frame absent/ambiguous at destination for the completed socket tuple")
    offset, _ = matches[0]
    return {"classification": "destination_kernel_ingress_not_native_parse_or_rejection",
            "frame_sha256": hashlib.sha256(frame).hexdigest(), "frame_bytes": len(frame),
            "message_id": message_id, "session": delivery["session"],
            "local": delivery["local"], "remote": delivery["remote"],
            "first_tcp_sequence": (anchor + offset) % 2**32,
            "socket_completion": delivery["classification"]}


def wire_receipt(before: dict, source: dict, kind: str, reply: dict,
                 observation: dict | None = None) -> dict:
    require(reply.get("qualification_only") is True
            and reply.get("kind") == kind, "required real wire probe unavailable")
    own = wire_endpoint(source["rpc"]["replication_address"])
    sender = wire_endpoint(reply.get("from"))
    selected = wire_endpoint(reply.get("native_selected_peer"))
    target = wire_endpoint(reply.get("target"))
    require(own["partition"] == before["partition"] and own["replica"] == source["replica"]
            and own["engine"] == source["rpc"]["engine_incarnation"],
            "Rust sender endpoint does not match the independently deployed native identity")
    require({k: v for k, v in own.items() if k != "engine"}
            == {k: v for k, v in sender.items() if k != "engine"}
            and (sender["engine"] != own["engine"] if kind == "wrong_sender"
                 else sender["engine"] == own["engine"]), "wire probe changed the wrong sender identity fields")
    peers = source["rpc"]["engine"].get("peers") or []
    if source["role"] == "Primary":
        require(any(peer["address"] == reply["native_selected_peer"] and peer["configured"]
                    and not peer["building"] and not peer["cleanup_required"] for peer in peers),
                "wire destination was not an actual completed native-configured peer")
    else:
        require(source["rpc"].get("observed_primary") == reply["native_selected_peer"]
                and source["rpc"]["engine"]["copy_done"], "secondary probe lacks its actual completed primary session")
    candidates = [replica for replica in before["replicas"]
                  if replica["replica"] == selected["replica"] and selected["partition"] == before["partition"]]
    require(len(candidates) == 1 and candidates[0]["replica"] != source["replica"],
            "native-selected wire peer is not in the queried partition")
    destination = candidates[0]
    require(destination["role"] == ("ActiveSecondary" if source["role"] == "Primary" else "Primary"),
            "native-selected peer role mismatch")
    if destination["rpc"]["engine_kind"] == "rust":
        published = wire_endpoint(destination["rpc"]["replication_address"])
        # REFrom has actor/address only; its learned primary cannot include
        # the public-only transport suffix. Do not equate that with identity.
        require({key: value for key, value in published.items() if key != "suffix"}
                == {key: value for key, value in selected.items() if key != "suffix"},
                "Rust destination differs from the native-selected endpoint")
    else:
        # Native EndOpen's GUID/ephemeral port are RA-owned, not the oracle's
        # fixture incarnation or its configured port-zero bind request.
        bind = urlsplit("//" + destination["rpc"]["replication_bind_address"])
        require(bind.hostname == selected["host"], "native-selected peer listener host differs from native telemetry")
    require({k: v for k, v in target.items() if k != "engine"}
            == {k: v for k, v in selected.items() if k != "engine"}
            and (target["engine"] != selected["engine"] if kind == "wrong_target"
                 else target["engine"] == selected["engine"]), "wire probe changed the wrong destination fields")
    epoch = source["rpc"]["native_epoch"]
    projected = {"data_loss": epoch["data_loss"], "configuration": epoch["configuration"] & ~0xFFFFFFFF}
    require(reply.get("native_epoch") == projected and type(reply.get("sequence")) is int
            and reply["sequence"] == source["rpc"]["engine"]["progress"]["assigned"] + 1
            and reply["sequence"] > 0, "wire probe native epoch/nonassigned nonzero candidate mismatch")
    sent = reply.get("sent_epoch")
    require(type(sent) is dict and set(sent) == {"data_loss", "configuration"}
            and all(type(value) is int and value >= 0 for value in sent.values())
            and ((sent["data_loss"], sent["configuration"]) < (projected["data_loss"], projected["configuration"])
                 if kind == "stale_epoch" else sent == projected), "wire probe did not exercise the requested epoch")
    delivery = wire_delivery(reply, own, selected, observation)
    return {"source_identity": identity(source), "destination_identity": identity(destination),
            "source_kind": source["rpc"]["engine_kind"], "destination_kind": destination["rpc"]["engine_kind"],
            "native_selected_peer": reply["native_selected_peer"],
            "wire_delivery": delivery,
            "address_evidence": "Rust native-configured session actor matches native partition/replica; "
                                "native listener host matches oracle bind telemetry (RA-owned GUID/port not invented)"}


def data_loss_invariants(before: dict, after: dict, ledger: Ledger, changed: bool, boundary: int = 5) -> Ledger:
    require(before["service"] == after["service"] and before["partition"] == after["partition"],
            "native data loss changed the owned service/partition")
    require(after["native_partition"]["epoch"]["data_loss"] > before["native_partition"]["epoch"]["data_loss"],
            "actual native full data-loss epoch did not advance")
    original = {identity(replica): replica for replica in before["replicas"]}
    survivors = [replica for replica in after["replicas"] if identity(replica) in original]
    require(survivors and any(identity(replica) not in original for replica in after["replicas"]),
            "native partial data loss requires an original survivor and a copied replacement")
    reconciled = Ledger()
    for key, record in ledger.records.items():
        require(record["committed"] and type(record["sequence"]) is int, "auxiliary seed lacked quorum success")
        if not changed or record["sequence"] <= boundary:
            reconciled.records[key] = dict(record)
    expected_last = boundary if changed else before["primary"]["rpc"]["state"]["last"]
    require(len(ledger.records) >= 20 and sorted(record["sequence"] for record in ledger.records.values())
            == list(range(1, len(ledger.records) + 1)), "fresh auxiliary requires at least 20 contiguous acknowledged writes")
    reconciled.compare(after)
    callbacks = []
    for replica in after["replicas"]:
        state = replica["rpc"]["state"]
        require(state["last"] == expected_last and state["history_count"] == len(reconciled.records),
                "provider retained a divergent tail or lied about reconciled progress/history")
        for field in ("data_loss_calls", "data_loss_changed_calls"):
            require(type(state.get(field)) is int and state[field] >= 0, "actual native data-loss callback counters required")
        require(state["data_loss_changed_calls"] <= state["data_loss_calls"], "invalid provider callback counters")
        require("last_data_loss_boundary" in state and (
                    state["last_data_loss_boundary"] is None or
                    type(state["last_data_loss_boundary"]) is int and state["last_data_loss_boundary"] >= 0),
                "actual provider last native data-loss boundary telemetry required")
        if changed and state["data_loss_changed_calls"]:
            require(state["last_data_loss_boundary"] == boundary, "native changed callback used the wrong provider boundary")
        if not changed:
            require(state["data_loss_changed_calls"] == 0 and state["last_data_loss_boundary"] is None,
                    "unarmed native OnDataLoss incorrectly changed provider state")
        old = original.get(identity(replica))
        if old is not None:
            previous = old["rpc"]["state"]
            require(state["data_loss_calls"] >= previous["data_loss_calls"]
                    and state["data_loss_changed_calls"] >= previous["data_loss_changed_calls"],
                    "provider callback counters reset within a surviving lifetime")
            if state["data_loss_calls"] > previous["data_loss_calls"]:
                callbacks.append(replica)
                if changed:
                    require(state["data_loss_changed_calls"] > previous["data_loss_changed_calls"]
                            and state["last_data_loss_boundary"] == boundary,
                            "armed actual native survivor callback did not report Changed=true at the boundary")
        if replica["rpc"]["engine_kind"] == "native-classic-v1":
            require(replica["rpc"]["native_current_progress"] == expected_last,
                    "native replicator progress differs from its actual reconciled provider")
    require(callbacks, "no actual native OnDataLoss callback increment on a queried original surviving provider")
    if any(replica["rpc"]["engine_kind"] == "native-classic-v1" for replica in before["replicas"]):
        require(before["native_partition"]["target"] == 3 and before["native_partition"]["minimum"] == 2
                and len(before["replicas"]) == 3
                and {replica["node"] for replica in before["replicas"]} == set(MIXED_NODES)
                and before["primary"]["rpc"]["node"] == "N0020"
                and before["primary"]["rpc"]["engine_kind"] == "rust",
                "mixed partial loss requires three ready replicas with the Rust primary selected for removal")
        require(len(survivors) == 1 and survivors[0]["rpc"]["engine_kind"] == "native-classic-v1"
                and after["primary"]["rpc"]["engine_kind"] == "native-classic-v1"
                and any(replica["role"] == "Primary" for replica in callbacks),
                "mixed partial loss did not establish the original native survivor as the new primary with an actual callback")
    primary = after["primary"]["rpc"]
    if primary["engine_kind"] == "rust":
        require(primary["engine"]["progress"]["assigned"] == expected_last
                and primary["engine"]["progress"]["applied"] == expected_last,
                "Rust primary queue did not reset to authoritative provider progress")
    return reconciled


def no_write_native_acks(value: dict, boundary: int) -> bool:
    source = value["primary"]["rpc"]
    if source["engine_kind"] != "rust":
        return False
    expected = {replica["replica"] for replica in value["replicas"] if replica["role"] == "ActiveSecondary"}
    peers = source["engine"]["peers"]
    return (len(expected) == 2 and len(peers) == 2
            and {peer["replica"] for peer in peers} == expected
            and source["state"]["last"] == boundary
            and source["engine"]["progress"]["assigned"] == boundary
            and source["engine"]["progress"]["committed"] == boundary
            and all(peer["applied"] >= boundary and peer["received"] >= boundary
                    and not peer["building"] and not peer["cleanup_required"] for peer in peers))


class CopyAckGate:
    """Keep actual CopyEnd ACK ownership separate from native build status."""
    def __init__(self, qualification, before: dict, removed: dict, evidence: dict):
        self.qualification, self.before, self.removed, self.evidence = qualification, before, removed, evidence
        self.endpoint = None
        self.replica = None
        self.previous_held_at = None
        self.negative_started = None
        self.negative_count = 0
        self.released = False
        evidence.update(hold_ms=6000, observations=[], released=False)

    @staticmethod
    def held(rpc: dict) -> bool:
        hold = rpc.get("copy_ack_hold", {})
        if hold.get("expired") or hold.get("cancelled"):
            raise RuntimeError("destination copy ACK hold expired/cancelled before explicit release")
        return (hold.get("held") is True and hold.get("copy_end_received") is True
                and hold.get("released") is False and hold.get("acknowledged") is False
                and type(hold.get("sequence")) is int and hold["sequence"] > 0
                and type(hold.get("boundary")) is int and hold["boundary"] > 0
                and rpc.get("state", {}).get("last") == hold["boundary"]
                and rpc.get("state", {}).get("copy_in_progress") is False
                and bool(rpc.get("state", {}).get("results")))

    @staticmethod
    def negative(value: dict, replica: dict, rpc: dict) -> dict:
        require(replica["status"] == "InBuild", "native build became Ready before destination copy ACK release")
        require(not rpc["engine"]["copy_done"] and not rpc["engine"]["ready"]
                and not rpc["engine"]["fault"] and not rpc["pump_fault"],
                "held copy ACK did not keep destination copy completion fenced")
        if not CopyAckGate.held(rpc):
            raise MissingEvidence("no independently received nonempty CopyEnd with its actual ACK held")
        boundary = rpc["copy_ack_hold"]["boundary"]
        primary = next(r for r in value["replicas"] if r["role"] == "Primary")["rpc"]
        if primary["engine_kind"] == "rust":
            peer = next((p for p in primary["engine"].get("peers", [])
                         if p["replica"] == replica["replica"]), None)
            if not (peer and peer.get("copy_horizon") == boundary and peer.get("copy_received") == -1):
                raise MissingEvidence("source has no exact destination copy EOS receipt")
            require(peer.get("copy_applied") != -1 and peer.get("building") is True,
                    "source completed native build on receive-only copy progress")
            require(type(peer.get("copy_applied")) is int
                    and peer["copy_applied"] < rpc["copy_ack_hold"]["sequence"]
                    and type(peer.get("copy_finish")) is int and peer["copy_finish"] > boundary
                    and peer["applied"] < peer["copy_finish"],
                    "held copy ACK or final live replication fence was already reported applied")
            return {"kind": "destination_EOS_receipt_at_source", "peer": peer}
        observation = next((o for o in primary.get("native_copy_horizons", [])
                            if o.get("requested_horizon") == boundary and o.get("enumeration_end") is True), None)
        if observation is None:
            raise MissingEvidence("native source has no actual copy enumeration EOS at received CopyEnd boundary")
        require(rpc["engine"]["progress"]["applied"] < primary["state"]["last"],
                "held destination already applied the live replication finish fence")
        return {"kind": "native_source_enumeration_EOS_and_destination_CopyEnd_receipt",
                "observation": observation}

    def observe(self, value: dict, native_started: float) -> None:
        if self.released:
            return
        replacements = [r for r in value["replicas"] if r["node"] == self.removed["node"]
                        and r["replica"] != self.removed["replica"]]
        if not replacements:
            return
        replica = replacements[0]
        origin = urlsplit(self.removed["address"])
        if self.endpoint is None:
            if replica["status"] == "Ready":
                raise MissingEvidence("replacement became Ready before a destination copy ACK hold was installed")
            lookup = f"{origin.scheme}://{origin.netloc}/data/{value['partition']}/{replica['replica']}/service"
            try:
                status, rpc = request(lookup, "diagnostics", timeout=1)
            except (OSError, http.client.HTTPException):
                return
            if status != 200:
                return
            canonical = urlsplit(rpc.get("service_address", ""))
            require((canonical.scheme, canonical.netloc) == (origin.scheme, origin.netloc)
                    and rpc.get("partition") == self.before["partition"]
                    and rpc.get("replica") == replica["replica"] and rpc.get("node") == replica["node"]
                    and rpc.get("engine_kind") == self.removed["rpc"]["engine_kind"]
                    and rpc.get("incarnation") is not None and rpc.get("engine_incarnation")
                    and rpc.get("host") and type(rpc.get("pid")) is int,
                    "InBuild destination diagnostics are not bound to the exact native replacement")
            self.endpoint, self.replica = rpc["service_address"], replica["replica"]
            # Retain exact native ownership before the hook request, including
            # a lost response or a lifetime that disappears while still InBuild.
            self.qualification.hook_identities[self.endpoint] = {
                "node": replica["node"], "replica": replica["replica"], "service": self.qualification.service}
            self.evidence["destination"] = rpc
            self.evidence["arm"] = self.qualification.hook(self.endpoint, "copy_ack_hold_ms", 6000)
        require(replica["replica"] == self.replica, "copy ACK gate destination was replaced before release")
        require(replica["status"] == "InBuild", "native build completed before explicit copy ACK release")
        status, rpc = request(self.endpoint, "diagnostics", timeout=1)
        require(status == 200 and rpc.get("replica") == self.replica
                and all(rpc.get(key) == self.evidence["destination"][key]
                        for key in ("partition", "node", "incarnation", "engine_incarnation", "host", "pid")),
                "held copy ACK diagnostics lifetime drifted")
        now = time.monotonic()
        held = self.held(rpc)
        if not held:
            self.evidence["last_destination_before_hold"] = rpc
            self.previous_held_at = None
            return
        operation = {key: rpc["copy_ack_hold"][key] for key in ("sequence", "boundary")}
        require(self.evidence.setdefault("held_operation", operation) == operation,
                "held destination copy operation changed before release")
        self.evidence["observations"].append({"at_monotonic": now, "native_started": native_started,
            "native_replica": replica, "destination": rpc})
        require(len(self.evidence["observations"]) <= 100, "bounded destination copy ACK observations")
        if held and self.previous_held_at is not None and native_started > self.previous_held_at:
            try:
                eos = self.negative(value, replica, rpc)
            except MissingEvidence:
                self.previous_held_at = now
                return
            self.negative_started = self.negative_started or now
            self.negative_count += 1
            self.evidence.update(source_eos=eos, negative_observations=self.negative_count,
                                 blocked_after_receive=True)
            # At least two native queries bracketed by actual held-ACK reads.
            if self.negative_count >= 2 and now - self.negative_started >= 1:
                self.evidence["release"] = self.qualification.hook(self.endpoint, "copy_ack_hold_ms", 0)
                self.released = True
                self.evidence.update(released=True, released_at_monotonic=time.monotonic())
        self.previous_held_at = now if held else None

    def complete(self, recovered: dict, workload: dict) -> None:
        if not self.released or not self.evidence.get("blocked_after_receive"):
            raise MissingEvidence("replacement copy lacks receive/held-ACK plus independent native InBuild negative evidence")
        replica = next((r for r in recovered["replicas"] if r["node"] == self.removed["node"]
                        and r["replica"] == self.replica), None)
        require(replica is not None and replica["status"] == "Ready", "held build did not complete on the same native replica")
        rpc = replica["rpc"]
        hold = rpc.get("copy_ack_hold", {})
        require(all(rpc.get(key) == self.evidence["destination"][key]
                    for key in ("partition", "node", "incarnation", "engine_incarnation", "host", "pid"))
                and all(hold.get(key) == self.evidence["held_operation"][key] for key in ("sequence", "boundary"))
                and hold.get("released") is True and hold.get("acknowledged") is True and hold.get("held") is False
                and hold.get("expired") is False and hold.get("cancelled") is False
                and rpc["engine"]["copy_done"] and not rpc["pump_fault"]
                and rpc["state"]["last"] == workload["last"]
                and rpc["state"]["last"] > hold["boundary"],
                "released copy ACK/final replication fence did not finish with the coherent live-write ledger")
        self.evidence.update(completed_native_replica=replica, coherent_workload=workload)


class Qualification:
    def __init__(self, allocation, bundle: Bundle, result: dict, mixed: bool, original_application=None):
        self.allocation, self.bundle, self.result, self.mixed = allocation, bundle, result, mixed
        self.service = result["application"] + "/Data"
        self.ledger = Ledger()
        self.client = Client(bundle, self.service, mixed, self.ledger)
        self.hooks: set[tuple[str, str]] = set()
        self.hook_identities: dict[str, dict] = {}
        self.paused: list[dict] = []
        self.serial = 0
        self.original_application = original_application
        self.auxiliaries: list[dict] = []

    def scenario(self, name: str, action) -> None:
        evidence = self.result["scenarios"][name]
        evidence.pop("reason", None)
        evidence.update(status="RUNNING", started_at=time.time())
        try:
            action(evidence)
            evidence["status"] = "PASS"
        except MissingEvidence as error:
            evidence.update(status="MISSING", error=str(error))
        except KeyboardInterrupt:
            evidence.update(status="FAIL", error="KeyboardInterrupt: qualification interrupted")
            raise
        except Exception as error:
            evidence.update(status="FAIL", error=f"{type(error).__name__}: {error}")
        finally:
            evidence["finished_at"] = time.time()
            evidence["duration_seconds"] = evidence["finished_at"] - evidence["started_at"]
            self.result["identified_workload"] = self.ledger.records
            if os.environ.get("FUBE_PROGRESS") == "1":
                print(json.dumps({"scenario": name, "status": evidence["status"],
                                  "error": evidence.get("error")}), file=sys.stderr, flush=True)

    def wait(self, *, accept=None, timeout: float = 180, capture: dict | None = None,
             service: str | None = None, target: int = 3, minimum: int = 2,
             mixed: bool | None = None) -> dict:
        service = service or self.service
        def check():
            value = native_snapshot(self.bundle, service)
            if service == self.service:
                for replica in value["replicas"]:
                    if replica.get("address"):
                        self.hook_identities[replica["address"]] = {
                            "node": replica["node"], "replica": replica["replica"], "service": service}
            if capture is not None:
                capture["last_native_observation"] = value
            assert_data(value, service, self.mixed if mixed is None else mixed, target, minimum)
            if accept is not None and not accept(value):
                return False
            return value
        return runtime.wait_condition(check, "native data placement/identity/access", timeout)

    def converge(self, evidence: dict) -> dict:
        def coherent(value):
            try:
                self.ledger.compare(value)
                return True
            except RuntimeError:
                return False
        value = self.wait(accept=coherent, capture=evidence)
        evidence.update(observation=value, workload=self.ledger.compare(value))
        return value

    def writes(self, count: int, prefix: str, stop: threading.Event | None = None) -> dict:
        self.serial += 1
        before = time.monotonic()
        records = [self.client.write(f"{prefix}-{self.serial}-{index}", stop=stop) for index in range(count)]
        return {"requested": count, "successful": len(records), "started_at_monotonic": before,
                "finished_at_monotonic": time.monotonic(),
                "ids": [record["id"] for record in records],
                "sequences": [record["sequence"] for record in records],
                "commit_times_monotonic": [next(attempt["at"] for attempt in reversed(record["attempts"])
                    if attempt.get("http_status") == 200 and attempt.get("reply", {}).get("outcome") == "committed")
                    for record in records]}

    @contextlib.contextmanager
    def writing(self, count: int, prefix: str, start: threading.Event | None = None):
        stop = threading.Event()
        def workload():
            while start is not None and not start.wait(.1):
                require(not stop.is_set(), "owned copy workload cancelled before native InBuild")
            require(not stop.is_set(), "owned copy workload cancelled")
            return self.writes(count, prefix, stop)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            worker = pool.submit(workload)
            try:
                yield worker
            finally:
                # On observation failure join only the bounded current operation,
                # not the remainder of an unbounded/background workload.
                stop.set()

    def hook(self, endpoint: str, name: str, value: int) -> dict:
        require(name in {"ack_delay_ms", "copy_delay_ms", "copy_ack_hold_ms"}
                and type(value) is int and 0 <= value <= {
                    "ack_delay_ms": 2000, "copy_delay_ms": 100, "copy_ack_hold_ms": 6000}[name],
                "qualification hook bounds")
        # Retain before sending: a lost response may have installed the hook.
        self.hooks.add((endpoint, name))
        status, reply = request(endpoint, "test", {name: value})
        require(status == 200 and reply.get("qualification_only") is True
                and reply.get(name) == value, f"real qualification hook unavailable: {name}")
        if value == 0:
            self.hooks.discard((endpoint, name))
        return reply

    def reset_hooks(self, endpoints: list[tuple[str, str]] | None = None) -> list:
        evidence = []
        for endpoint, name in list(self.hooks if endpoints is None else endpoints):
            try:
                evidence.append({"endpoint": endpoint, "hook": name, "reset": self.hook(endpoint, name, 0)})
            except Exception as error:
                # Only confirmed native departure may excuse a vanished lifetime.
                evidence.append({"endpoint": endpoint, "hook": name, "error": str(error)})
        return evidence

    def deployment_identity(self, evidence: dict) -> None:
        value = self.wait(capture=evidence)
        evidence["observation"] = value
        self.bundle.artifact("launchers/launch-mixed.py")
        evidence["selector_sha256"] = self.bundle.metadata["artifacts"]["launchers/launch-mixed.py"]["sha256"]
        evidence["node_engines"] = NODE_ENGINES
        evidence["processes"] = []
        for replica in value["replicas"]:
            expected = process_expected(self.allocation, self.bundle, self.service, replica, self.mixed)
            captured = inspect_process(self.allocation, expected)
            evidence["processes"].append({"native_identity": expected, "process": captured})
        if not self.mixed:
            evidence["default_stateless"] = runtime.wait_observe(
                self.result["application"] + "/Singleton", observer=self.bundle.observer)
            evidence["default_empty"] = runtime.wait_stateful(
                self.result["application"] + "/Stateful", 1, 1, observer=self.bundle.observer)
            evidence["mixed_stateless_create"] = self.allocation.sf(
                "service", "create", "--app-id", self.result["application"].removeprefix("fabric:/"),
                "--name", self.result["application"] + "/MixedStateless",
                "--service-type", "RustRuntimeType", "--stateless", "--instance-count", "-1",
                "--activation-mode", "SharedProcess", "--singleton-scheme")
            evidence["mixed_stateless"] = self.stateless_siblings()
            for replica in value["replicas"]:
                sibling = next(item for item in evidence["mixed_stateless"] if item["rpc"]["node"] == replica["node"])
                require(sibling["native_pid"] == replica["rpc"]["pid"]
                        and sibling["rpc"]["host"] == replica["rpc"]["host"],
                        "RR data/stateless units did not share the native code-package host")
        self.client.endpoint = value["primary"]["endpoint"]

    def stateless_siblings(self) -> list:
        return runtime.wait_mixed(self.result["application"] + "/MixedStateless", self.bundle.observer)

    def unchanged_stateless(self, before: list) -> list:
        after = self.stateless_siblings()
        for original in before:
            current = next(item for item in after if item["rpc"]["node"] == original["rpc"]["node"])
            runtime.require_same_instance(current, original)
        return after

    def writes_100(self, evidence: dict) -> None:
        # A nonempty 300-item snapshot gives independently queried live copy a
        # useful observation window at the 20 ms/item qualification delay.
        evidence["writes"] = self.writes(300, "initial")
        self.converge(evidence)
        require(evidence["workload"]["successful"] >= 100
                and all(sequence > 0 for sequence in evidence["writes"]["sequences"]), "100 nonzero commits required")

    def identified_retry(self, evidence: dict) -> None:
        original = next(record for record in self.ledger.records.values() if record["committed"])
        baseline = self.converge(evidence)
        status, reply = request(baseline["primary"]["endpoint"], "write",
                                {key: original[key] for key in ("id", "key", "value")})
        self.ledger.reply(original, status, reply)
        require(reply["duplicate"] is True and reply["sequence"] == original["sequence"],
                "identified duplicate was admitted again")
        evidence["identical_retry"] = reply
        conflict = {key: original[key] for key in ("id", "key", "value")}
        conflict["value"] += "-conflict"
        status, reply = request(baseline["primary"]["endpoint"], "write", conflict)
        require(status in (400, 503) and reply.get("outcome") == "rejected"
                and "sequence" not in reply, "conflicting identity was not rejected before admission")
        evidence["conflicting_retry"] = {"http_status": status, "reply": reply}
        self.converge(evidence)

    def move(self, evidence: dict, kind: str | None = None) -> dict:
        before = self.wait(capture=evidence)
        candidates = [replica for replica in before["replicas"]
                      if replica["role"] == "ActiveSecondary"
                      and (kind is None or replica["rpc"]["engine_kind"] == kind)]
        require(bool(candidates), "planned MovePrimary requires an observed ActiveSecondary of the requested engine")
        target = candidates[0]["node"]
        response = runtime.run(str(self.bundle.observer), "127.0.0.1:19000", self.service,
                               "--move-primary", target, timeout=75)
        evidence["move_request"] = strict_json(response)
        after = self.wait(capture=evidence, accept=lambda value:
                          value["primary"]["rpc"]["node"] == target
                          and runtime.epoch_of(value) > runtime.epoch_of(before))
        status, _ = request(before["primary"]["endpoint"])
        old = next((replica for replica in after["replicas"]
                    if replica["replica"] == before["primary"]["rpc"]["replica"]), None)
        require(status == 503 and old is not None and old["role"] == "ActiveSecondary"
                and old["rpc"]["pid"] == before["primary"]["native_pid"],
                "old primary route must become 503 without replacing the process")
        evidence.update(before=before, after=after, old_primary_http_status=status)
        self.client.endpoint = after["primary"]["endpoint"]
        return after

    def planned_move(self, evidence: dict) -> None:
        self.move(evidence)
        evidence["post_move_writes"] = self.writes(5, "planned-move")
        self.converge(evidence)

    def remove(self, value: dict, replica: dict) -> None:
        require(replica in value["replicas"] and replica["node"] in NODE_ENGINES
                and type(replica["replica"]) is int and replica["replica"] > 0,
                "unbound native replica removal")
        # No --force-remove. Partition, replica and node come from one native query.
        self.allocation.sf("replica", "remove", "--node-name", replica["node"],
                           "--partition-id", value["partition"], "--replica-id", str(replica["replica"]),
                           timeout=40)

    @staticmethod
    def copy_evidence(samples: list[dict], removed: dict) -> dict:
        in_build = []
        horizons = []
        peer_observations = []
        for sample in samples:
            value = sample.get("native")
            if not isinstance(value, dict):
                continue
            for replica in value["replicas"]:
                if replica["status"] == "InBuild" and (replica["node"], replica["replica"]) != (
                        removed["node"], removed["replica"]):
                    in_build.append({"at": sample["at_monotonic"], "node": replica["node"],
                                     "replica": replica["replica"], "role": replica["role"]})
                rpc = replica.get("rpc", {})
                for peer in rpc.get("engine", {}).get("peers") or []:
                    if peer.get("copy_horizon") is not None:
                        horizons.append({"at": sample["at_monotonic"], "source": replica["node"], **peer})
                for field in ("copy_horizon", "copy_snapshot_horizon"):
                    if rpc.get(field) is not None:
                        horizons.append({"at": sample["at_monotonic"], "source": replica["node"],
                                         "copy_horizon": rpc[field]})
                for observation in rpc.get("native_copy_horizons") or []:
                    if observation.get("requested_horizon") is not None:
                        horizons.append({"at": sample["at_monotonic"], "source": replica["node"],
                            "copy_horizon": observation["requested_horizon"],
                            "native_provider_observation": observation})
                if rpc.get("observed_primary") or rpc.get("engine", {}).get("peers"):
                    peer_observations.append({"at": sample["at_monotonic"], "node": replica["node"],
                        "observed_primary": rpc.get("observed_primary"), "peers": rpc.get("engine", {}).get("peers")})
        return {"native_in_build": in_build, "horizons": horizons, "peer_observations": peer_observations}

    def copy(self, evidence: dict, *, target_kind: str | None = None,
             cancel: bool = False, move_during_copy: bool = False, hold_copy_ack: bool = False) -> None:
        before = self.converge(evidence)
        candidates = [replica for replica in before["replicas"] if replica["role"] == "ActiveSecondary"
                      and (target_kind is None or replica["rpc"]["engine_kind"] == target_kind)]
        require(bool(candidates) and before["primary"]["rpc"]["state"]["last"] > 0,
                "nonempty copy requires an observed secondary of the target engine")
        removed = candidates[0]
        endpoint = before["primary"]["endpoint"]
        evidence.update(before=before, removed_identity=identity(removed), samples=[])
        siblings = self.stateless_siblings() if not self.mixed else None
        if siblings is not None:
            evidence["stateless_siblings_before"] = siblings
        copy_sources = ([replica["address"] for replica in before["replicas"]]
                        if move_during_copy else [endpoint])
        evidence["hooks"] = [self.hook(source, "copy_delay_ms", 20) for source in copy_sources]
        gate = None
        if hold_copy_ack:
            require(not cancel and not move_during_copy, "copy ACK gate requires an uninterrupted native build")
            evidence["destination_copy_ack"] = {}
            gate = CopyAckGate(self, before, removed, evidence["destination_copy_ack"])
        self.remove(before, removed)
        first_build = None
        started = time.monotonic()
        begin_writes = threading.Event()
        move_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1) if move_during_copy else None
        move_request = None
        try:
            # Use surplus writes so at least 100 completed writes can be proven
            # inside the independently observed build/horizon interval.
            with self.writing(200, "live-copy", begin_writes) as writer:
                deadline = started + 180
                action_done = not (cancel or move_during_copy)
                while time.monotonic() < deadline:
                    stamp = time.monotonic()
                    try:
                        value = native_snapshot(self.bundle, self.service)
                    except (RuntimeError, OSError) as error:
                        evidence["samples"].append({"at_monotonic": stamp, "error": str(error)})
                        require(len(evidence["samples"]) < 500, "bounded copy observation count")
                        time.sleep(.05)
                        continue
                    evidence["samples"].append({"at_monotonic": stamp,
                        "query_finished_at_monotonic": time.monotonic(), "native": value})
                    if gate is not None:
                        gate.observe(value, stamp)
                    replacements = [replica for replica in value["replicas"]
                        if replica["status"] == "InBuild" and (
                            replica["node"], replica["replica"]) != (removed["node"], removed["replica"])]
                    if replacements and first_build is None:
                        first_build = replacements[0]
                        evidence["first_in_build"] = first_build
                        if not cancel:
                            begin_writes.set()
                    if first_build is not None and not action_done and move_request is None:
                        if move_during_copy:
                            # The destination is an existing ready secondary, never the unfinished build.
                            eligible = [r for r in value["replicas"] if r["role"] == "ActiveSecondary"
                                        and r["status"] == "Ready"]
                            require(bool(eligible), "no ready secondary for an interrupted-copy epoch transition")
                            move_request = move_pool.submit(runtime.run,
                                str(self.bundle.observer), "127.0.0.1:19000", self.service,
                                "--move-primary", eligible[0]["node"], timeout=75)
                        else:
                            self.remove(value, first_build)
                            evidence["cancelled_native_departure"] = runtime.wait_native_replica_absent(
                                self.bundle.observer, self.service, first_build["node"], first_build["replica"])
                            action_done = True
                    if move_request is not None and move_request.done() and not action_done:
                        evidence["interrupt_move"] = strict_json(move_request.result())
                        action_done = True
                    if cancel and action_done and first_build is not None and any(
                            (replica["node"], replica["replica"]) !=
                            (first_build["node"], first_build["replica"]) for replica in replacements):
                        # The cancelled build is not the positive build window.
                        # Start the live workload only on its actual successor.
                        begin_writes.set()
                    ready = all(r["status"] == "Ready" for r in value["replicas"])
                    if writer.done() and ready and len(value["replicas"]) == 3 and action_done:
                        if gate is not None and not gate.released:
                            raise MissingEvidence("native replacement became Ready without the delayed-copy-ACK negative gate")
                        break
                    require(len(evidence["samples"]) < 500, "bounded copy observation count")
                    time.sleep(0.05)
                require(action_done, "native InBuild/cancellation was not observed")
                evidence["concurrent_writes"] = writer.result(timeout=45)
            recovered = self.converge(evidence)
            if gate is not None:
                gate.complete(recovered, evidence["workload"])
            evidence["removed_native_departure"] = runtime.wait_native_replica_absent(
                self.bundle.observer, self.service, removed["node"], removed["replica"])
            replacement = next((replica for replica in recovered["replicas"] if (
                replica["node"], replica["replica"]) != (removed["node"], removed["replica"])
                and identity(replica) not in {identity(r) for r in before["replicas"]}), None)
            require(replacement is not None and identity(replacement) != identity(removed),
                    "replacement has no new native/engine identity")
            expected = process_expected(self.allocation, self.bundle, self.service, replacement, self.mixed)
            evidence["replacement"] = {"native_identity": expected, "process": inspect_process(self.allocation, expected),
                                       "replica": replacement}
            if siblings is not None:
                evidence["stateless_siblings_after"] = self.unchanged_stateless(siblings)
            live = self.copy_evidence(evidence["samples"], removed)
            evidence["live_copy"] = live
            require(evidence["concurrent_writes"]["successful"] >= 100 and bool(live["native_in_build"]),
                    "100 live writes and native InBuild must actually be observed")
            writes = evidence["concurrent_writes"]
            require(any(writes["started_at_monotonic"] <= sample["at"] <= writes["finished_at_monotonic"]
                        for sample in live["native_in_build"]), "live writes did not overlap native build")
            if move_during_copy:
                require(runtime.epoch_of(recovered) > runtime.epoch_of(before), "copy interruption did not change native epoch")
            if not live["horizons"]:
                raise MissingEvidence("native InBuild and concurrent writes executed, but no observed requested copy horizon")
            require(any(horizon["copy_horizon"] > 0 for horizon in live["horizons"]), "copy horizon was empty")
            window_start = max(min(sample["at"] for sample in live["native_in_build"]),
                               min(sample["at"] for sample in live["horizons"]))
            window_end = min(max(sample["at"] for sample in live["native_in_build"]),
                             max(sample["at"] for sample in live["horizons"]))
            observed_commits = sum(window_start <= stamp <= window_end for stamp in writes["commit_times_monotonic"])
            evidence["live_copy"]["commits_inside_observed_build_horizon_window"] = observed_commits
            require(observed_commits >= 100, "fewer than 100 commits occurred inside observed native build/copy horizon")
            require(any(sum(sequence > horizon["copy_horizon"] for sequence in writes["sequences"]) >= 100
                        for horizon in live["horizons"]), "fewer than 100 writes beyond the pinned copy horizon")
        finally:
            if move_pool is not None:
                move_pool.shutdown(wait=True, cancel_futures=True)
            evidence["hook_cleanup"] = self.reset_hooks([(source, "copy_delay_ms") for source in copy_sources])
            if gate is not None and gate.endpoint is not None:
                evidence["copy_ack_hook_cleanup"] = self.reset_hooks([(gate.endpoint, "copy_ack_hold_ms")])

    def apply_ack_delay(self, evidence: dict) -> None:
        before = self.converge(evidence)
        secondaries = [r for r in before["replicas"] if r["role"] == "ActiveSecondary"]
        require(len(secondaries) == 2, "ACK delay must cover both remote quorum candidates")
        endpoints = [(r["address"], "ack_delay_ms") for r in secondaries]
        evidence["delayed_native_targets"] = [{"node": r["node"], "replica": r["replica"],
                                              "engine": r["rpc"]["engine_kind"]} for r in secondaries]
        evidence["hooks"] = [self.hook(endpoint, name, 2000) for endpoint, name in endpoints]
        self.serial += 1
        request_id = f"ack-delay-{self.serial}"
        baseline = before["primary"]["rpc"]["engine"]["progress"]["committed"]
        evidence["samples"] = []
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                started = time.monotonic()
                write = pool.submit(self.client.write, request_id)
                blocked_after_receive = False
                while not write.done() and time.monotonic() - started < 8:
                    replicas = []
                    for target in before["replicas"]:
                        status, rpc = request(target["address"], "diagnostics")
                        require(status == 200, "ACK observation route lost its owned lifetime")
                        replicas.append({"node": target["node"], "role": target["role"], "rpc": rpc})
                    elapsed = time.monotonic() - started
                    evidence["samples"].append({"elapsed_seconds": elapsed, "replicas": replicas,
                                                "client_completed": write.done()})
                    primary = next(r["rpc"] for r in replicas if r["role"] == "Primary")
                    received = all(request_id in r["rpc"]["state"]["results"]
                                   and r["rpc"]["engine"]["progress"]["received"] > baseline
                                   for r in replicas if r["role"] == "ActiveSecondary")
                    if received and elapsed >= .25 and not write.done():
                        require(primary["engine"]["progress"]["committed"] == baseline,
                                "receipt-only observations advanced quorum commitment")
                        blocked_after_receive = True
                    time.sleep(.025)
                record = write.result(timeout=45)
                evidence.update(write=record, elapsed_seconds=time.monotonic() - started,
                                blocked_after_receive=blocked_after_receive)
            require(blocked_after_receive and evidence["elapsed_seconds"] >= 1.8
                    and record["committed"], "actual receive/apply ACK delay did not block client quorum success")
            self.converge(evidence)
        finally:
            evidence["hook_cleanup"] = self.reset_hooks(endpoints)

    def unavailable_quorum(self, evidence: dict) -> None:
        before = self.converge(evidence)
        evidence["before"] = before
        primary = before["primary"]
        secondaries = [replica for replica in before["replicas"] if replica["role"] == "ActiveSecondary"]
        require(len(secondaries) == 2 and len({r["rpc"]["pid"] for r in before["replicas"]}) == 3,
                "unavailable quorum requires independently hosted replicas")
        evidence["paused_processes"] = []
        self.serial += 1
        record = self.ledger.mutation(f"no-quorum-{self.serial}")
        try:
            for replica in secondaries:
                expected = process_expected(self.allocation, self.bundle, self.service, replica, self.mixed)
                expected["captured"] = inspect_process(self.allocation, expected)
                # Capture before the fallible signal response, for unconditional resume.
                self.paused.append(expected)
                evidence["paused_processes"].append(inspect_process(self.allocation, expected, "pause"))
            sid = self.service.removeprefix("fabric:/").replace("/", "~")
            native = runtime.native_read(self.allocation, f"/Services/{quote(sid, safe='')}/$/GetPartitions?api-version=6.0")
            evidence["native_while_unavailable"] = native
            partitions = native if isinstance(native, list) else native["Items"]
            require(len(partitions) == 1 and partitions[0]["TargetReplicaSetSize"] == 3
                    and partitions[0]["MinReplicaSetSize"] == 2,
                    "unavailable quorum test changed native configuration size")
            started = time.monotonic()
            record["attempts"].append({"at": started, "outcome": "acceptance_unknown_until_HTTP_reply",
                                       "native_quorum_unavailable": True})
            status, reply = request(primary["endpoint"], "write",
                                    {key: record[key] for key in ("id", "key", "value")}, timeout=9)
            outcome = self.ledger.reply(record, status, reply)
            require(status != 200 and outcome in {"rejected", "ambiguous"}, "unavailable quorum falsely committed")
            status, diagnostics = request(primary["endpoint"], "diagnostics")
            require(status == 200 and diagnostics["engine"]["progress"]["committed"]
                    == primary["rpc"]["engine"]["progress"]["committed"],
                    "unavailable quorum advanced commitment from sends/receipts")
            evidence.update(write=record, duration_seconds=time.monotonic() - started, diagnostics=diagnostics)
        finally:
            evidence["resume"] = self.resume()
        self.converge(evidence)
        # Never turn the uncertain write into a success merely because it applied.
        evidence["accepted_write_outcome_after_recovery"] = record

    def resume(self) -> list:
        evidence = []
        for expected in list(self.paused):
            try:
                value = inspect_process(self.allocation, expected, "resume")
                self.paused.remove(expected)
                evidence.append(value)
            except Exception as error:
                evidence.append({"native_identity": expected, "error": str(error)})
        return evidence

    def lost_reply_dedup(self, evidence: dict) -> None:
        before = self.converge(evidence)
        self.serial += 1
        record = self.ledger.mutation(f"lost-reply-{self.serial}")
        endpoint = before["primary"]["endpoint"]
        url = urlsplit(endpoint)
        path = url.path.rsplit("/", 1)[0] + "/write?" + urlencode(
            {key: record[key] for key in ("id", "key", "value")})
        with socket.create_connection((url.hostname, url.port), timeout=3) as connection:
            connection.sendall(f"GET {path} HTTP/1.1\r\nHost: {url.hostname}:{url.port}\r\nConnection: close\r\n\r\n".encode())
            # Intentionally do not consume the response; acceptance is unknown.
        record["attempts"].append({"outcome": "ambiguous_transport", "reply_intentionally_unread": True})
        def replicated(value):
            states = [r["rpc"]["state"] for r in value["replicas"] if r["status"] == "Ready"]
            return len(states) == 3 and all(record["id"] in state["results"] for state in states)
        applied = self.wait(accept=replicated, capture=evidence)
        record["sequence"] = applied["primary"]["rpc"]["state"]["results"][record["id"]]
        evidence["before_move"] = applied
        self.move(evidence)
        status, reply = request(self.client.endpoint, "write", {key: record[key] for key in ("id", "key", "value")})
        outcome = self.ledger.reply(record, status, reply)
        require(outcome in {"ambiguous", "committed"} and reply["duplicate"] is True,
                "lost-reply retry did not retain the same accepted identity")
        evidence["retry_outcome"] = outcome
        self.converge(evidence)

    def abrupt_primary_loss(self, evidence: dict) -> None:
        before = self.converge(evidence)
        old = next(replica for replica in before["replicas"] if replica["role"] == "Primary")
        siblings = self.stateless_siblings() if not self.mixed else None
        if siblings is not None:
            sibling = next(item for item in siblings if item["rpc"]["node"] == old["node"])
            require(sibling["native_pid"] == old["rpc"]["pid"] and sibling["rpc"]["host"] == old["rpc"]["host"],
                    "abrupt loss fixture has no colocated stateless unit")
            evidence["stateless_siblings_before"] = siblings
        expected = process_expected(self.allocation, self.bundle, self.service, old, self.mixed)
        expected["captured"] = inspect_process(self.allocation, expected)
        evidence["native_identity"] = expected
        evidence["kill"] = inspect_process(self.allocation, expected, "kill")
        after = self.wait(capture=evidence, accept=lambda value:
            value["primary"]["rpc"]["host"] != old["rpc"]["host"]
            and value["primary"]["rpc"]["engine_incarnation"] != old["rpc"]["engine_incarnation"]
            and runtime.epoch_of(value) > runtime.epoch_of(before))
        self.client.endpoint = after["primary"]["endpoint"]
        evidence["process_wide_not_per_unit"] = True
        evidence["post_failure_writes"] = self.writes(5, "abrupt-primary-loss")
        self.converge(evidence)
        evidence["lost_reply_retries_after_recovery_copy"] = []
        for record in list(self.ledger.records.values()):
            if any(attempt.get("reply_intentionally_unread") for attempt in record["attempts"]):
                original_sequence = record["sequence"]
                status, reply = request(self.client.endpoint, "write",
                                        {key: record[key] for key in ("id", "key", "value")})
                outcome = self.ledger.reply(record, status, reply)
                require(outcome in {"ambiguous", "committed"} and reply.get("duplicate") is True
                        and reply["sequence"] == original_sequence,
                        "recovery copy expired a lost-reply identity or changed its sequence")
                evidence["lost_reply_retries_after_recovery_copy"].append(reply)
        new_primary = next(replica for replica in after["replicas"] if replica["role"] == "Primary")
        fresh = process_expected(self.allocation, self.bundle, self.service, new_primary, self.mixed)
        evidence["new_primary_native_identity"] = fresh
        evidence["new_primary_process"] = inspect_process(self.allocation, fresh)
        require(fresh["host"] != expected["host"], "abrupt recovery did not have a new native host identity")
        previous = {identity(replica) for replica in before["replicas"]}
        replacements = [replica for replica in after["replicas"] if identity(replica) not in previous]
        require(bool(replacements), "killed host had no independently observed replacement")
        evidence["replacement_hosts"] = []
        for replica in replacements:
            replacement = process_expected(self.allocation, self.bundle, self.service, replica, self.mixed)
            require(replacement["host"] != expected["host"], "killed host identity was reused")
            evidence["replacement_hosts"].append({
                "native_identity": replacement, "process": inspect_process(self.allocation, replacement)})
        if not self.mixed:
            after_siblings = self.stateless_siblings()
            for original in siblings:
                current = next(item for item in after_siblings if item["rpc"]["node"] == original["rpc"]["node"])
                if original["rpc"]["node"] == old["node"]:
                    require(current["rpc"]["host"] != original["rpc"]["host"]
                            and current["native_entry_point_instance"] != original["native_entry_point_instance"],
                            "process-wide loss did not replace the colocated stateless host")
                else:
                    runtime.require_same_instance(current, original)
            evidence["stateless_siblings_after"] = after_siblings
            evidence["default_stateless_recovered"] = runtime.wait_observe(
                self.result["application"] + "/Singleton", observer=self.bundle.observer)
            evidence["default_empty_recovered"] = runtime.wait_stateful(
                self.result["application"] + "/Stateful", 1, 1, observer=self.bundle.observer)

    def per_unit_close(self, evidence: dict) -> None:
        before = self.converge(evidence)
        siblings = self.stateless_siblings() if not self.mixed else None
        primary = before["primary"]["rpc"]
        auxiliary = self.result["application"] + "/CloseIsolation"
        evidence["create"] = create_data_service(
            self.allocation, self.result["application"], auxiliary, (primary["node"],), 1, 1)
        # The auxiliary need not itself contain both engine kinds.
        aux = self.wait(service=auxiliary, target=1, minimum=1, mixed=False if primary["engine_kind"] == "rust" else None,
                        capture=evidence, accept=lambda value: value["primary"]["rpc"]["pid"] == primary["pid"])
        require(aux["primary"]["rpc"]["host"] == primary["host"], "per-unit Close did not share the main data host")
        self.allocation.sf("service", "delete", "--service-id", auxiliary.removeprefix("fabric:/").replace("/", "~"))
        app_id = self.result["application"].removeprefix("fabric:/")
        runtime.wait_condition(lambda: not any(item["name"] == auxiliary for item in runtime.items(
            self.allocation.sf("service", "list", "--application-id", app_id))),
            "native auxiliary deletion", 60)
        evidence["native_departure"] = runtime.wait_native_replica_absent(self.bundle.observer, auxiliary,
            aux["primary"]["rpc"]["node"], aux["primary"]["rpc"]["replica"])
        after = self.converge(evidence)
        unchanged = next(replica for replica in after["replicas"] if replica["role"] == "Primary")
        require(identity(unchanged) == identity(next(replica for replica in before["replicas"] if replica["role"] == "Primary")),
                "per-unit deletion replaced an unrelated main data unit")
        status, _ = request(aux["primary"]["endpoint"])
        require(status in (404, 503), "deleted unit retained a usable owned route")
        evidence.update(auxiliary_before=aux, unrelated_before=before, unrelated_after=after,
                        old_auxiliary_http_status=status)
        if siblings is not None:
            evidence["stateless_siblings_before"] = siblings
            evidence["stateless_siblings_after"] = self.unchanged_stateless(siblings)

    def stale_wire_receipts(self, evidence: dict) -> None:
        evidence.update(coverage=[], admission_is_not_rejection_evidence=True,
                        limitation="No native-primary packet spoofing. Rust probes use only existing native-selected sessions. "
                                   "Exact socket write/flush plus independent destination kernel ingress are required; "
                                   "neither proves native parsing or rejection.")
        for primary_kind in (("rust", "native-classic-v1") if self.mixed else ("rust",)):
            group = {"primary_kind": primary_kind, "receipts": [], "positive_copy": {}}
            evidence["coverage"].append(group)
            current = self.converge(group)
            if current["primary"]["rpc"]["engine_kind"] != primary_kind:
                group["native_move"] = {}
                self.move(group["native_move"], primary_kind)
            # A demoted continuing secondary learns the new primary session
            # from real traffic, not merely from a role callback.
            group["session_establishing_write"] = self.writes(1, "wire-session")
            roles = ("Primary",) if primary_kind == "rust" and self.mixed else ("ActiveSecondary",)
            if not self.mixed:
                roles = ("Primary", "ActiveSecondary")
            for role in roles:
                kinds = ("stale_epoch", "wrong_sender", "wrong_target") if role == "Primary" else (
                    "wrong_sender", "stale_copy_ack", "wrong_target")
                for kind in kinds:
                    case = {"kind": kind, "rust_role": role, "samples": []}
                    group["receipts"].append(case)
                    baseline = self.converge(case)
                    # Let already-completed ACK/queue housekeeping settle before
                    # measuring a negative packet with no concurrent writer.
                    def settled(value):
                        nonlocal baseline
                        try:
                            stable_data(baseline, value)
                        except RuntimeError:
                            baseline = value
                            return False
                        return True
                    before = self.wait(accept=settled, capture=case)
                    source = next((replica for replica in before["replicas"]
                                   if replica["role"] == role and replica["rpc"]["engine_kind"] == "rust"), None)
                    require(source is not None, "required Rust wire-probe source is not natively placed in this direction")
                    case["before"] = before
                    with WireCapture(self.allocation, wire_capture_scope(source), case) as capture:
                        status, reply = request(source["address"], "test", {"wire_probe": kind},
                                                timeout=WIRE_CAPTURE_SECONDS - 4)
                        case["receipt"] = {"http_status": status, "reply": reply}
                        require(status == 200, f"required actual wire probe API failed: {kind}, HTTP {status}: {reply}")
                        observation = capture.finish()
                        case["peer_correlation"] = wire_receipt(before, source, kind, reply, observation)
                    if self.mixed:
                        require(case["peer_correlation"]["destination_kind"] == "native-classic-v1",
                                "mixed negative probe did not actually target the native engine")
                    for _ in range(3):
                        time.sleep(.2)
                        after = assert_data(native_snapshot(self.bundle, self.service), self.service, self.mixed)
                        case["samples"].append(after)
                        stable_data(before, after)
                        self.ledger.compare(after)
                    case["negative_observation"] = "Native identities/roles/full epochs and actual provider ledger/state/engine progress unchanged."
                    case["positive_write"] = self.writes(1, "after-wire-" + kind)
                    require(case["positive_write"]["sequences"] == [before["primary"]["rpc"]["state"]["last"] + 1],
                            "negative packet consumed a real assignment or blocked the next matching write")
                    self.converge(case)
            target_kind = "native-classic-v1" if primary_kind == "rust" and self.mixed else "rust"
            self.copy(group["positive_copy"], target_kind=target_kind)

    def guard_application(self) -> None:
        original = self.original_application
        require(original is not None and original.name == self.result["application"],
                "auxiliary mutation requires the originally captured native application/capability")
        require(native_application_identity(self.allocation, original.name, original.type_identity,
                                            original.capability) == original,
                "original native application instance/capability drift; no auxiliary mutation authorized")

    def auxiliary_partition(self, auxiliary: dict, *, allow_absent: bool = False) -> str | None:
        service = auxiliary["service"]
        require(service.startswith(self.result["application"] + "/DataLoss")
                and service != self.service, "data loss is restricted to this run's auxiliary service")
        if allow_absent:
            description = native_management(self.allocation, "GET",
                application_path(self.result["application"]) + "/$/GetServices/"
                + quote(service.removeprefix("fabric:/").replace("/", "~"), safe="")
                + "?api-version=6.0")
            if description["http_status"] == 204 and description["body"] is None:
                return None
            if description["http_status"] == 404 and isinstance(description["body"], dict) and (
                    description["body"].get("Error", {}).get("Code") in {
                        "FABRIC_E_SERVICE_DOES_NOT_EXIST", "FABRIC_E_APPLICATION_NOT_FOUND"}):
                return None
            body = native_success(description, 200, "owned auxiliary service absence/identity")
            require(isinstance(body, dict) and body.get("Name") == service and body.get("ServiceKind") == "Stateful",
                    "native auxiliary service resource identity drift")
        reply = native_management(self.allocation, "GET",
                                  service_path(service) + "/$/GetPartitions?api-version=6.0")
        if allow_absent and reply["http_status"] == 404 and isinstance(reply["body"], dict) and (
                reply["body"].get("Error", {}).get("Code") == "FABRIC_E_SERVICE_DOES_NOT_EXIST"):
            return None
        body = native_success(reply, 200, "owned auxiliary partition query")
        partitions = body if isinstance(body, list) else body.get("Items") if isinstance(body, dict) else None
        require(isinstance(partitions, list) and len(partitions) == 1
                and not (isinstance(body, dict) and body.get("ContinuationToken")),
                "owned auxiliary singleton partition query incomplete/ambiguous")
        item = partitions[0]
        require(item.get("ServiceKind") == "Stateful" and isinstance(item.get("PartitionInformation"), dict),
                "owned auxiliary partition is not a native stateful service")
        partition = native_uuid(item["PartitionInformation"].get("Id"))
        if auxiliary.get("partition") is not None:
            require(partition == auxiliary["partition"], "auxiliary name reused with a different native partition; retain")
        return partition

    @staticmethod
    def remember_auxiliary(auxiliary: dict, value: dict) -> None:
        require(value["service"] == auxiliary["service"] and value["partition"] == auxiliary["partition"],
                "auxiliary observation identity drift")
        for replica in value["replicas"]:
            endpoint = replica.get("address")
            if endpoint and endpoint not in auxiliary["original_routes"]:
                auxiliary["original_routes"].append(endpoint)

    def create_auxiliary(self, changed: bool, evidence: dict) -> dict:
        self.guard_application()
        service = self.result["application"] + "/DataLoss" + ("Changed" if changed else "Unchanged") + uuid.uuid4().hex
        require(not any(item["name"] == service for item in runtime.items(self.allocation.sf(
            "service", "list", "--application-id", self.result["application"].removeprefix("fabric:/")))),
            "minted auxiliary service collision; no adoption")
        auxiliary_mixed = self.mixed and self.result.get("profile") != "full"
        nodes = (MIXED_NODES if auxiliary_mixed else
                 tuple(node for node, kind in NODE_ENGINES.items() if kind == "rust") if self.mixed else tuple(NODE_ENGINES))
        auxiliary = {"service": service, "partition": None, "changed": changed,
                     "target": 3, "minimum": 2, "mixed": auxiliary_mixed, "eligible_nodes": nodes,
                     "application_native_id": self.original_application.native_application_id,
                     "original_routes": [], "create_state": "requested", "cleanup": {}}
        # A lost create response must remain visible to the original-app cleanup.
        self.auxiliaries.append(auxiliary)
        self.result.setdefault("auxiliary_data_services", []).append(auxiliary)
        evidence["auxiliary"] = auxiliary
        auxiliary["create"] = create_data_service(
            self.allocation, self.result["application"], service, nodes, auxiliary["target"], auxiliary["minimum"])
        auxiliary["create_state"] = "accepted"
        value = self.wait(service=service, target=auxiliary["target"], minimum=auxiliary["minimum"],
                          mixed=auxiliary_mixed, capture=evidence)
        self.guard_application()
        auxiliary["partition"] = self.auxiliary_partition(auxiliary)
        native_uuid(auxiliary["partition"])
        self.remember_auxiliary(auxiliary, value)
        evidence["initial_observation"] = value
        return auxiliary

    def move_auxiliary_primary(self, auxiliary: dict, before: dict, target_node: str,
                               seed: Ledger, evidence: dict) -> dict:
        self.guard_application()
        require(auxiliary["application_native_id"] == self.original_application.native_application_id
                and self.auxiliary_partition(auxiliary) == before["partition"],
                "native MovePrimary requires the original application's owned auxiliary partition")
        service = auxiliary["service"]
        assert_data(before, service, auxiliary["mixed"], auxiliary["target"], auxiliary["minimum"])
        targets = [replica for replica in before["replicas"]
                   if replica["role"] == "ActiveSecondary" and replica["status"] == "Ready"
                   and replica["node"] in auxiliary["eligible_nodes"]]
        selected = next((replica for replica in targets if replica["node"] == target_node), None)
        require(selected is not None, "auxiliary MovePrimary requires an observed eligible ActiveSecondary")
        evidence.update(service=service, partition=auxiliary["partition"],
                        application_native_id=auxiliary["application_native_id"], before=before,
                        target=target_node, eligible_targets=[{
                            "identity": identity(replica), "node": replica["node"], "role": replica["role"],
                            "engine_kind": replica["rpc"]["engine_kind"], "epoch": replica["rpc"]["native_epoch"]}
                            for replica in targets])
        response = runtime.run(str(self.bundle.observer), "127.0.0.1:19000", service,
                               "--move-primary", target_node, timeout=75)
        reply = evidence["move_request"] = strict_json(response)
        require(reply.get("request") == "MovePrimary" and reply.get("partition") == auxiliary["partition"]
                and reply.get("target") == target_node and reply.get("constraints") == "enforced"
                and reply.get("native_request_completed") is True,
                "native auxiliary MovePrimary did not confirm the owned partition/target with constraints enforced")
        original = {identity(replica) for replica in before["replicas"]}
        def settled(value):
            require(value["partition"] == auxiliary["partition"], "auxiliary MovePrimary partition identity drift")
            seed.compare(value)
            return (value["primary"]["rpc"]["node"] == target_node
                    and runtime.epoch_of(value) > runtime.epoch_of(before)
                    and {identity(replica) for replica in value["replicas"]} == original
                    and all(replica["rpc"]["state"]["last"] == 20
                            and (replica["role"] == "Primary"
                                 or replica["rpc"]["engine"]["progress"]["applied"] == 20)
                            and (replica["rpc"]["engine_kind"] != "native-classic-v1"
                                 or replica["rpc"]["native_current_progress"] == 20)
                            for replica in value["replicas"]))
        after = self.wait(service=service, target=auxiliary["target"], minimum=auxiliary["minimum"],
                          mixed=auxiliary["mixed"], capture=evidence, accept=settled)
        assert_data(after, service, auxiliary["mixed"], auxiliary["target"], auxiliary["minimum"])
        require(settled(after), "auxiliary MovePrimary did not settle with all 20 writes applied in the original lifetimes")
        self.remember_auxiliary(auxiliary, after)
        evidence["after"] = after
        return after

    def cleanup_auxiliary(self, auxiliary: dict) -> None:
        cleanup = auxiliary["cleanup"]
        if auxiliary.get("fas"):
            cancel_fas(self.allocation, auxiliary["fas"])
        if cleanup.get("native_absence_confirmed"):
            self.retired_auxiliary_routes(auxiliary)
            return
        self.guard_application()
        require(auxiliary["application_native_id"] == self.original_application.native_application_id,
                "auxiliary was not created under the captured original native application")
        require(auxiliary.get("partition") is not None,
                "partial auxiliary creation has no captured partition; original-app cleanup required")
        partition = self.auxiliary_partition(auxiliary, allow_absent=True)
        if partition is not None:
            cleanup["delete_requested"] = True
            self.allocation.sf("service", "delete", "--service-id",
                               auxiliary["service"].removeprefix("fabric:/").replace("/", "~"))
        runtime.wait_condition(lambda: self.auxiliary_partition(auxiliary, allow_absent=True) is None,
                               "original auxiliary native absence", timeout=60)
        cleanup["native_absence_confirmed"] = True
        self.retired_auxiliary_routes(auxiliary)

    @staticmethod
    def retired_auxiliary_routes(auxiliary: dict) -> None:
        require(auxiliary["cleanup"].get("native_absence_confirmed") is True,
                "original auxiliary route retirement requires independent native absence")
        observations = []
        auxiliary["cleanup"]["original_routes_retired"] = False
        auxiliary["cleanup"]["retired_routes"] = observations
        for endpoint in auxiliary["original_routes"]:
            try:
                status, body = request(endpoint)
                observations.append({"endpoint": endpoint, "http_status": status, "body": body})
                require(status in (404, 503), "deleted auxiliary retained a usable original route")
            except (OSError, http.client.HTTPException) as error:
                observations.append({"endpoint": endpoint, "unreachable_after_native_absence": str(error)})
        auxiliary["cleanup"]["original_routes_retired"] = True

    def cleanup_auxiliaries(self) -> list:
        outcomes = []
        for auxiliary in self.auxiliaries:
            try:
                self.cleanup_auxiliary(auxiliary)
                outcomes.append({"service": auxiliary["service"], "cleanup": auxiliary["cleanup"]})
            except Exception as error:
                auxiliary["cleanup"]["error"] = f"{type(error).__name__}: {error}"
                outcomes.append({"service": auxiliary["service"], "error": auxiliary["cleanup"]["error"]})
        return outcomes

    def confirm_auxiliaries_absent(self) -> None:
        for auxiliary in self.auxiliaries:
            require(self.auxiliary_partition(auxiliary, allow_absent=True) is None,
                    "auxiliary remains after original-app cleanup")
            auxiliary["cleanup"]["native_absence_confirmed"] = True
            self.retired_auxiliary_routes(auxiliary)

    def data_loss_case(self, evidence: dict, changed: bool) -> None:
        first = len(self.auxiliaries)
        try:
            self._data_loss_case(evidence, changed)
        finally:
            original_error = sys.exc_info()[1]
            for auxiliary in self.auxiliaries[first:]:
                if auxiliary["cleanup"].get("native_absence_confirmed") and auxiliary["cleanup"].get("original_routes_retired"):
                    continue
                try:
                    self.cleanup_auxiliary(auxiliary)
                except Exception as error:
                    evidence["cleanup_error"] = f"{type(error).__name__}: {error}"
                    if original_error is None:
                        raise
        main_after_delete = self.converge(evidence)
        stable_data(evidence["unrelated_main_before"], main_after_delete)
        evidence["unrelated_main_after_auxiliary_delete"] = main_after_delete
        if "stateless_siblings_before" in evidence:
            evidence["stateless_siblings_after_auxiliary_delete"] = self.unchanged_stateless(
                evidence["stateless_siblings_before"])

    def _data_loss_case(self, evidence: dict, changed: bool) -> None:
        unrelated_before = self.converge(evidence)
        siblings = self.stateless_siblings() if not self.mixed else None
        auxiliary = self.create_auxiliary(changed, evidence)
        service, target, minimum = auxiliary["service"], auxiliary["target"], auxiliary["minimum"]
        require(target == 3 and minimum == 2, "native partial loss requires auxiliary membership bound to target 3/minimum 2")
        seed = Ledger()
        client = Client(self.bundle, service, auxiliary["mixed"], seed, target, minimum)
        client.endpoint = evidence["initial_observation"]["primary"]["endpoint"]
        evidence["seed_writes"] = [dict(client.write(f"seed-{index}")) for index in range(20)]
        def coherent(ledger):
            def accept(value):
                try:
                    ledger.compare(value)
                    return True
                except RuntimeError:
                    return False
            return accept
        settled_seed = None
        def seeded_and_stable(value):
            nonlocal settled_seed
            if not coherent(seed)(value) or not all(
                    replica["rpc"]["state"]["last"] == 20
                    and (replica["role"] == "Primary"
                         or replica["rpc"]["engine"]["progress"]["applied"] == 20)
                    for replica in value["replicas"]):
                return False
            previous, settled_seed = settled_seed, value
            if previous is None:
                return False
            try:
                stable_data(previous, value)
                return True
            except RuntimeError:
                return False
        before = self.wait(service=service, target=target, minimum=minimum, mixed=auxiliary["mixed"],
                           capture=evidence, accept=seeded_and_stable)
        self.remember_auxiliary(auxiliary, before)
        if auxiliary["mixed"]:
            moves = evidence["pre_fas_moves"] = []
            if before["primary"]["rpc"]["node"] == "N0020":
                native_secondary = next((replica for replica in before["replicas"]
                                         if replica["role"] == "ActiveSecondary"
                                         and replica["rpc"]["engine_kind"] == "native-classic-v1"), None)
                require(native_secondary is not None, "no observed native secondary to prepare the auxiliary Rust primary move")
                moves.append({})
                before = self.move_auxiliary_primary(auxiliary, before, native_secondary["node"], seed, moves[-1])
            moves.append({})
            before = self.move_auxiliary_primary(auxiliary, before, "N0020", seed, moves[-1])
            require(before["primary"]["rpc"]["node"] == "N0020"
                    and before["primary"]["rpc"]["engine_kind"] == "rust",
                    "mixed auxiliary must have the observed Rust primary before arming/FAS")
        evidence.update(before=before, seed_workload=seed.compare(before),
                        actual_engine_kinds=sorted({replica["rpc"]["engine_kind"] for replica in before["replicas"]}),
                        expected_survivor_engine_kind="native-classic-v1" if auxiliary["mixed"] else "rust",
                        pre_fas_roles_epochs=[{
                            "identity": identity(replica), "node": replica["node"], "role": replica["role"],
                            "engine_kind": replica["rpc"]["engine_kind"], "epoch": replica["rpc"]["native_epoch"]}
                            for replica in before["replicas"]],
                        explicit_data_loss_scope="Only this fresh auxiliary may lose acknowledged volatile writes; main ledger is never reset.")
        require(before["primary"]["rpc"]["state"]["last"] == 20, "fresh auxiliary provider seed progress differs")
        for replica in before["replicas"]:
            state = replica["rpc"]["state"]
            require(type(state.get("data_loss_calls")) is int and state["data_loss_calls"] >= 0
                    and type(state.get("data_loss_changed_calls")) is int and state["data_loss_changed_calls"] == 0
                    and state.get("last_data_loss_boundary") is None, "actual fresh provider callback counters unavailable")
        if changed:
            evidence["arming"] = []
            for replica in before["replicas"]:
                armed = {"native_identity": identity(replica), "endpoint": replica["address"], "boundary": 5}
                evidence["arming"].append(armed)
                status, reply = request(replica["address"], "test", {"data_loss_boundary": 5})
                armed.update(http_status=status, reply=reply)
                require(status == 200 and reply.get("qualification_only") is True
                        and reply.get("armed_only") is True and reply.get("data_loss_boundary") == 5,
                        "required native-callback-only data-loss arming API unavailable")
            armed = self.wait(service=service, target=target, minimum=minimum, mixed=auxiliary["mixed"], capture=evidence)
            stable_data(before, armed)
            evidence["after_arming_without_callback"] = armed
        self.guard_application()
        require(self.auxiliary_partition(auxiliary) == before["partition"],
                "native FAS target differs from the original observed auxiliary")
        operation = {"operation_id": str(uuid.uuid4()), "service": service, "partition": auxiliary["partition"],
                     "application_native_id": auxiliary["application_native_id"],
                     "state": "StartRequested", "progress": []}
        evidence["native_fas"] = operation
        try:
            system = runtime.items(self.allocation.sf("service", "list", "--application-id", "System"))
            fas_available = any(item["name"] == "fabric:/System/FaultAnalysisService" for item in system)
            if fas_available:
                auxiliary["fas"] = operation
                operation["start_path"] = fas_path(service, auxiliary["partition"], operation["operation_id"], "StartDataLoss")
                operation["start_reply"] = native_management(self.allocation, "POST", operation["start_path"])
                operation["start_http_status"] = operation["start_reply"]["http_status"]
                native_success(operation["start_reply"], 202, "StartDataLoss PartialDataLoss")
                evidence["completed_progress"] = poll_fas(self.allocation, operation)
            else:
                # The Linux11.8 native validator excludes FAS configuration.
                # Use only native per-unit RemoveReplica for the exact original
                # auxiliary members. No callback/election/transport-rule shortcut.
                operation.update(state="NotSubmitted", native_cleanup_terminal=True,
                                 service_available=False)
                self.native_partial_loss_removals(auxiliary, before, evidence)
            # The FAS terminal response also confirms native transport-fault
            # cleanup. No forced cancellation or user-created transport fault.
            after = self.wait(service=service, target=target, minimum=minimum, mixed=auxiliary["mixed"], capture=evidence,
                accept=lambda value: value["native_partition"]["epoch"]["data_loss"]
                    > before["native_partition"]["epoch"]["data_loss"])
            self.remember_auxiliary(auxiliary, after)
            evidence["after_native_data_loss"] = after
            reconciled = data_loss_invariants(before, after, seed, changed)
            evidence["reconciled_workload"] = reconciled.compare(after)
            evidence["actual_survivor_callbacks"] = [{
                "identity": identity(replica), "engine_kind": replica["rpc"]["engine_kind"],
                "state": replica["rpc"]["state"]} for replica in after["replicas"]
                if identity(replica) in {identity(original) for original in before["replicas"]}
                and replica["rpc"]["state"]["data_loss_calls"] > next(
                    original["rpc"]["state"]["data_loss_calls"] for original in before["replicas"]
                    if identity(original) == identity(replica))]
            continued = Client(self.bundle, service, auxiliary["mixed"], reconciled, target, minimum)
            continued.endpoint = after["primary"]["endpoint"]
            first = continued.write("post-native-data-loss-0")
            require(first["sequence"] == (6 if changed else 21),
                    "actual next native-driven assignment ignored authoritative provider progress")
            evidence["subsequent_writes"] = [dict(first)] + [
                dict(continued.write(f"post-native-data-loss-{index}")) for index in range(1, 4)]
            final = self.wait(service=service, target=target, minimum=minimum, mixed=auxiliary["mixed"],
                              capture=evidence, accept=coherent(reconciled))
            self.remember_auxiliary(auxiliary, final)
            evidence.update(final_observation=final, final_workload=reconciled.compare(final))
            require(all(replica["rpc"]["state"]["last"] == (9 if changed else 24) for replica in final["replicas"]),
                    "post-data-loss replacement providers did not converge to actual continued writes")
            recovered = {identity(replica): replica["rpc"]["state"] for replica in after["replicas"]}
            for replica in final["replicas"]:
                previous = recovered.get(identity(replica))
                if previous is not None:
                    state = replica["rpc"]["state"]
                    require(all(state[field] >= previous[field] for field in ("data_loss_calls", "data_loss_changed_calls"))
                            and (previous["last_data_loss_boundary"] is None
                                 or state["last_data_loss_boundary"] == previous["last_data_loss_boundary"]),
                            "actual provider callback counters/boundary reset after reconciliation/copy/continued writes")
            main_after = self.converge(evidence)
            stable_data(unrelated_before, main_after)
            evidence.update(unrelated_main_before=unrelated_before, unrelated_main_after=main_after)
            if siblings is not None:
                evidence.update(stateless_siblings_before=siblings,
                                stateless_siblings_after=self.unchanged_stateless(siblings))
        finally:
            self.cleanup_auxiliary(auxiliary)

    def native_partial_loss_removals(self, auxiliary: dict, before: dict, evidence: dict) -> None:
        require(before["service"] == auxiliary["service"] and before["partition"] == auxiliary["partition"]
                and len(before["replicas"]) == 3, "original auxiliary native partial-loss binding")
        primary = next(replica for replica in before["replicas"] if replica["role"] == "Primary")
        secondary = next(replica for replica in before["replicas"] if replica["role"] == "ActiveSecondary")
        targets = [primary, secondary]
        evidence["native_removal_driver"] = {
            "mechanism": "concurrent native RemoveReplica, no FAS/no transport fault/no manual callback",
            "targets": [{"node": r["node"], "replica": r["replica"], "identity": identity(r)} for r in targets],
            "started_at_monotonic": time.monotonic(),
        }
        self.guard_application()
        require(self.auxiliary_partition(auxiliary) == before["partition"],
                "original auxiliary partition changed before native loss")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            requests = [pool.submit(self.remove, before, replica) for replica in targets]
            for result in requests:
                result.result(timeout=45)
        evidence["native_removal_driver"]["accepted_requests"] = len(requests)
        evidence["native_removed_departures"] = [
            runtime.wait_native_replica_absent(self.bundle.observer, auxiliary["service"],
                                              replica["node"], replica["replica"])
            for replica in targets]

    def authoritative_data_loss(self, evidence: dict) -> None:
        evidence.update(unchanged={}, changed={}, driver="Native FAS if deployed; otherwise exact native per-unit concurrent removals. Never a manual callback/election",
                        source_contract="service-fabric 51a7703a1f83749a9234d3ceb4296d8622e42cca: "
                        "httpgateway/FaultsHandler.cpp (StartDataLoss, GetDataLossProgress, CancelTestCommand); "
                        "Constants.cpp:398-406; managed/FabricFAS/.../InvokeDataLossSteps.cs:90-104,270-320; "
                        "FaultAnalysisServiceUtility.cs:565-595 GetReplicasForPartialLoss removes the primary plus "
                        "floor(live replicas/2) secondaries: two loses both; three with a Rust primary leaves one native survivor")
        self.data_loss_case(evidence["unchanged"], False)
        self.data_loss_case(evidence["changed"], True)

    def direction(self, kind: str, evidence: dict) -> None:
        value = self.wait(capture=evidence)
        evidence["move"] = {}
        if value["primary"]["rpc"]["engine_kind"] != kind:
            value = self.move(evidence["move"], kind)
        else:
            # Establish the role through a native MovePrimary in this direction,
            # not by relying on initial placement.
            other = "native-classic-v1" if kind == "rust" else "rust"
            self.move(evidence["move"], other)
            evidence["move_back"] = {}
            value = self.move(evidence["move_back"], kind)
        require(value["primary"]["rpc"]["engine_kind"] == kind, "native MovePrimary direction mismatch")
        other = "native-classic-v1" if kind == "rust" else "rust"
        evidence["nonempty_writes"] = self.writes(10, "direction-" + kind)
        evidence["apply_ack_delay"] = {}
        self.apply_ack_delay(evidence["apply_ack_delay"])
        evidence["replacement_copy"] = {}
        self.copy(evidence["replacement_copy"], target_kind=other, hold_copy_ack=True)
        after = self.converge(evidence)
        # No client write may be used to establish a newly promoted session.
        # The freshly copied opposite-engine replica must support native
        # movement and then movement back with the exact existing prefix.
        evidence["post_copy_no_write_moves"] = []
        boundary = after["primary"]["rpc"]["state"]["last"]
        original_replicas = {identity(replica) for replica in after["replicas"]}
        for selected_kind in (other, kind, other, kind):
            moved = {"requested_engine": selected_kind, "no_intervening_client_writes": True}
            evidence["post_copy_no_write_moves"].append(moved)
            moved_after = self.move(moved, selected_kind)
            if selected_kind == "rust":
                moved_after = self.wait(capture=moved, accept=lambda value: no_write_native_acks(value, boundary))
                moved["actual_fresh_native_ack_frontiers"] = moved_after["primary"]["rpc"]["engine"]["peers"]
            self.ledger.compare(moved_after)
            require({identity(replica) for replica in moved_after["replicas"]} == original_replicas
                    and all(replica["rpc"]["state"]["last"] == boundary for replica in moved_after["replicas"]),
                    "post-copy no-write movement changed replica lifetimes or reused/advanced a sequence")
        after = self.converge(evidence)
        members = {replica["replica"] for replica in after["replicas"]}
        for replica in after["replicas"]:
            if replica["role"] == "Primary" and replica["rpc"]["engine_kind"] == "rust":
                require(all(peer["replica"] in members for peer in replica["rpc"]["engine"]["peers"]),
                        "retired configuration peer retained after replacement convergence")
        evidence["native_issued_peer_observations"] = [{
            "node": r["node"], "replica": r["replica"], "role": r["role"],
            "epoch": r["rpc"]["native_epoch"], "replication_address": r["rpc"]["replication_address"],
            "observed_primary": r["rpc"].get("observed_primary"),
            "peers": r["rpc"]["engine"].get("peers"),
        } for r in after["replicas"]]
        rust = [r for r in after["replicas"] if r["rpc"]["engine_kind"] == "rust"]
        if not any(r["rpc"].get("observed_primary") or any(peer.get("address") for peer in
                   r["rpc"]["engine"].get("peers", [])) for r in rust):
            raise MissingEvidence("data/copy ran but Rust peer addresses/observed-primary identity are not exposed")

    def ledger_capacity_backpressure(self, evidence: dict) -> None:
        before = self.converge(evidence)
        endpoint = before["primary"]["endpoint"]
        self.client.endpoint = endpoint
        deadline = time.monotonic() + 300
        rejected = None
        admitted = 0
        # This is deliberately last: the partition-lifetime ledger cannot be
        # cleared to make later scenarios pass.
        for index in range(4097):
            require(time.monotonic() < deadline, "bounded live ledger saturation deadline")
            record = self.ledger.mutation(f"ledger-capacity-{index}")
            status, reply = request(endpoint, "write", {key: record[key] for key in ("id", "key", "value")})
            outcome = self.ledger.reply(record, status, reply)
            if outcome == "rejected":
                require("provider" in reply.get("error", "").lower()
                        and any(token in reply.get("error", "").lower()
                                for token in ("identity ledger", "history", "resource")),
                        "rejection was not actual provider capacity/backpressure")
                rejected = record
                break
            if outcome == "ambiguous":
                self.client.write(record["id"])
            admitted += 1
        require(rejected is not None, "new identities were not rejected at the finite provider limit")
        evidence.update(new_successful_writes=admitted, capacity_rejection=rejected)
        full = self.converge(evidence)
        state = full["primary"]["rpc"]["state"]
        require(len(state["results"]) == 4096 and state["history_count"] == 4096
                and state["retained_bytes"] <= 16 * 1024 * 1024,
                "observed finite provider history/identity capacity differs")
        original = next(record for record in self.ledger.records.values() if record["committed"])
        status, reply = request(endpoint, "write", {key: original[key] for key in ("id", "key", "value")})
        outcome = self.ledger.reply(original, status, reply)
        require(outcome in {"ambiguous", "committed"} and reply.get("duplicate") is True
                and reply["sequence"] == original["sequence"], "capacity expired a previously committed identity")
        evidence["old_identity_retry_at_capacity"] = reply
        self.converge(evidence)

    def run(self) -> None:
        self.scenario("deployment_identity", self.deployment_identity)
        if self.result["scenarios"]["deployment_identity"]["status"] != "PASS":
            # Nothing below can qualify a peer that did not activate and prove
            # its real native deployment/executable identity. Preserve MISSING
            # rather than spending every scenario timeout on the same failure.
            self.result["errors"].append("deployment identity failed; remaining scenarios not executed")
            return
        self.scenario("writes_100", self.writes_100)
        if self.result["scenarios"]["writes_100"]["status"] != "PASS":
            self.result["errors"].append("baseline writes failed; dependent scenarios not executed")
            return
        actions = (
            ("identified_retry", self.identified_retry),
            ("planned_move", self.planned_move),
            ("apply_ack_delay", self.apply_ack_delay),
            ("replacement_copy", self.copy),
            ("copy_cancellation_recovery", lambda evidence: self.copy(evidence, cancel=True)),
            ("lost_reply_dedup", self.lost_reply_dedup),
            ("unavailable_quorum", self.unavailable_quorum),
            ("abrupt_primary_loss", self.abrupt_primary_loss),
            ("per_unit_close", self.per_unit_close),
            ("stale_wire_receipts", self.stale_wire_receipts),
            ("authoritative_data_loss", self.authoritative_data_loss),
        )
        for name, action in actions:
            self.scenario(name, action)
        if self.mixed:
            self.scenario("rust_primary_native_secondary", lambda evidence: self.direction("rust", evidence))
            self.scenario("native_primary_rust_secondary", lambda evidence: self.direction("native-classic-v1", evidence))
            self.scenario("native_interrupted_copy_epoch",
                          lambda evidence: self.copy(evidence, move_during_copy=True))
        self.scenario("ledger_capacity_backpressure", self.ledger_capacity_backpressure)
        self.result["identified_workload"] = self.ledger.records


def provenance(bundle: Bundle) -> dict:
    def sha_env(name):
        value = os.environ.get(name) or None
        require(value is None or re.fullmatch(r"[a-f0-9]{40}", value), f"invalid provenance SHA: {name}")
        return value
    return {
        "executable_source_revision": bundle.source_revision,
        "bundle_manifest_sha256": bundle.manifest_sha256,
        "review_head": sha_env("FUBE_REVIEW_HEAD"),
        "ci_head": sha_env("FUBE_CI_HEAD"),
        "ci_merge": sha_env("FUBE_CI_MERGE"),
        "ci_run_id": os.environ.get("FUBE_CI_RUN_ID") or os.environ.get("GITHUB_RUN_ID"),
        "qualification_script_sha256": runtime.digest(Path(__file__)),
        "note": "Review/CI revisions are caller labels, not executable or successful CI evidence.",
    }


UPLOAD_MAX_BYTES = 512 * 1024 * 1024
UPLOAD_MAX_ENTRIES = 64
UPLOAD_SECONDS = 180

UPLOAD = r"""
import ctypes,hashlib,json,os,re,secrets,signal,stat,subprocess,sys,time
path,action=sys.argv[1:3]; proof=json.loads(sys.argv[3])
def require(value,message):
    if not value: raise RuntimeError(message)
def recovery_note(error,message):
    message=message[:16384]
    if hasattr(error,"add_note"):error.add_note(message)
    else:print(message,file=sys.stderr,flush=True)
if action=="transfer":
    deadline=float(sys.argv[4]); token=sys.argv[5]
    require(0<deadline-time.time()<=180 and re.fullmatch(r"[a-f0-9]{64}",token),
            "invalid bounded upload helper lifetime")
    def expired(*_): raise TimeoutError("descriptor-bound transfer deadline")
    signal.signal(signal.SIGALRM,expired)
    # A pidfd-bound stop must unwind subprocess.run, killing/reaping the SDK
    # apphost before this helper releases its original source descriptors.
    signal.signal(signal.SIGTERM,expired)
    signal.setitimer(signal.ITIMER_REAL,deadline-time.time())
    with open("/proc/self/stat") as status:
        start_time=status.read().rsplit(") ",1)[1].split()[19]
    print(json.dumps({"pid":os.getpid(),"start_time":start_time,"token":token}),flush=True)
parent,name=os.path.split(path)
require(parent=="/var/lib/sf-node" and re.fullmatch(r"fube-data-[a-f0-9]{32}",name),
        "invalid data upload reservation path")
parent_fd=os.open(parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
directory_fd=None
owner_fd=None
private=None
private_identity=None
private_created=False
published=False
def identity(fd):
    info=os.fstat(fd)
    return [info.st_dev,info.st_ino]
def owner(fd):
    file=os.open(".fube-upload-owner",os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=fd)
    try:
        info=os.fstat(file)
        require(stat.S_ISREG(info.st_mode) and 64<=info.st_size<=65536 and info.st_nlink==1,
                "upload owner file is not the original regular capability")
        return identity(file),os.read(file,64).decode("ascii")
    finally: os.close(file)
def verify(entry):
    info=os.stat(entry,dir_fd=parent_fd,follow_symlinks=False)
    ancestor=os.stat("..",dir_fd=directory_fd,follow_symlinks=False)
    parent_info=os.stat(parent,follow_symlinks=False)
    require(stat.S_ISDIR(info.st_mode) and [info.st_dev,info.st_ino]==proof["directory"]
            and identity(directory_fd)==proof["directory"]
            and identity(parent_fd)==proof["parent"]
            and [parent_info.st_dev,parent_info.st_ino]==proof["parent"]
            and [ancestor.st_dev,ancestor.st_ino]==proof["parent"],
            "upload directory incarnation changed; retain")
    file,capability=owner(directory_fd)
    require(file==proof["owner_file"] and capability==proof["capability"],
            "upload owner capability changed; retain")
def publish(source,destination,source_fd=None,destination_fd=None):
    libc=ctypes.CDLL(None,use_errno=True)
    rename=getattr(libc,"renameat2",None)
    require(rename is not None,"atomic renameat2(RENAME_NOREPLACE) unavailable; retain")
    rename.argtypes=[ctypes.c_int,ctypes.c_char_p,ctypes.c_int,ctypes.c_char_p,ctypes.c_uint]
    rename.restype=ctypes.c_int
    if rename(parent_fd if source_fd is None else source_fd,os.fsencode(source),
              parent_fd if destination_fd is None else destination_fd,os.fsencode(destination),1)!=0:
        error=ctypes.get_errno()
        raise OSError(error,os.strerror(error),destination)
def recorded_owner():
    file=os.open(".fube-upload-owner",os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=directory_fd)
    try:
        require(identity(file)==proof["owner_file"],"upload inventory owner changed; retain")
        require(owner(directory_fd)==(proof["owner_file"],proof["capability"]),
                "upload inventory capability changed; retain")
        return file
    except BaseException:
        os.close(file)
        raise
def append_record(file,record):
    data=("\n"+json.dumps(record,separators=(",",":"))).encode("ascii")
    require(os.fstat(file).st_size+len(data)<=65536,"upload inventory byte bound")
    os.lseek(file,0,os.SEEK_END)
    require(os.write(file,data)==len(data),"short upload inventory write")
    os.fsync(file)
def inspect_tree(records,directories):
    expected={entry:set() for entry in directories}
    expected[""].add(".fube-upload-owner")
    for record in records:
        entry=record["path"]; ancestor,_,leaf=entry.rpartition("/")
        fd=directories[ancestor]
        info=os.stat(leaf,dir_fd=fd,follow_symlinks=False)
        require([info.st_dev,info.st_ino]==record["identity"]
                and (stat.S_ISDIR(info.st_mode) if record["kind"]=="directory"
                     else stat.S_ISREG(info.st_mode)),
                "upload child incarnation changed; retain")
        if record["kind"]=="directory":
            require(identity(directories[entry])==record["identity"],
                    "original upload child descriptor changed; retain")
        expected[ancestor].add(leaf)
    for entry,fd in directories.items():
        require(set(os.listdir(fd))==expected[entry],"upload child inventory changed; retain")
def file_hash(fd):
    digest=hashlib.sha256(); offset=0
    while True:
        chunk=os.pread(fd,65536,offset)
        if not chunk: return digest.hexdigest()
        offset+=len(chunk)
        require(offset<=512*1024*1024,"upload source hash byte bound")
        digest.update(chunk)
def transfer():
    # Only the original root descriptor authorizes writes. Never reopen the
    # public root/children for capture. Directory creation/capture happens at
    # unpublished nonces, then RENAME_NOREPLACE publishes the captured inode.
    directories={"":directory_fd}; files={}; records=[]; held=[]
    seen=set(); total=0; entries=0
    stream=sys.stdin.buffer
    def hold(fd):
        held.append(fd)
        require(len(held)<=160,"upload descriptor inventory bound")
        return fd
    def read_exact(size):
        data=stream.read(size)
        require(len(data)==size,"truncated bounded upload archive")
        return data
    def number(field):
        value=field.rstrip(b"\0 ").lstrip(b" ")
        require(re.fullmatch(b"[0-7]+",value) is not None,"invalid upload tar number")
        return int(value,8)
    try:
        journal=hold(recorded_owner())
        require(os.fstat(journal).st_size==64,"upload transfer already attempted; retain")
        if set(os.listdir(directory_fd))!={".fube-upload-owner"}:
            raise FileExistsError("preexisting upload child; refuse adoption; retain")
        while True:
            block=read_exact(512)
            if block==b"\0"*512:
                require(read_exact(512)==b"\0"*512,"invalid upload tar terminator")
                # The host emits USTAR's bounded final record padding only.
                padding=stream.read(10241)
                require(len(padding)<=10240 and not padding.strip(b"\0"),
                        "upload archive trailing data/bound")
                break
            entries+=1
            require(entries<=64,"upload archive entry bound")
            require(block[257:265]==b"ustar\x0000"
                    and not block[345:500].strip(b"\0")
                    and number(block[148:156])==sum(block[:148])+8*32+sum(block[156:]),
                    "strict USTAR upload header required")
            raw=block[:100].split(b"\0",1)[0]
            entry=raw.decode("ascii")
            kind=block[156:157]; size=number(block[124:136]); mode=number(block[100:108])
            if kind==b"5": entry=entry.removesuffix("/")
            parts=entry.split("/")
            require(0<len(entry)<=99 and len(parts)<=8
                    and all(re.fullmatch(r"[A-Za-z0-9_.-]+",part)
                            and part not in (".","..") for part in parts)
                    and parts[0] not in (".fube-upload-owner",".fube-upload-marker")
                    and "_.dir" not in parts and entry not in seen,
                    "invalid/duplicate upload archive path")
            require(kind in (b"0",b"\0",b"5") and not block[157:257].strip(b"\0"),
                    "upload archive permits regular files/directories only")
            require((kind==b"5" and size==0 and mode==0o700)
                    or (kind!=b"5" and mode in (0o644,0o755)),
                    "invalid upload archive size/mode")
            total+=size
            require(total<=512*1024*1024,"upload archive byte bound")
            ancestor="/".join(parts[:-1])
            require(ancestor in directories,"upload archive ancestor was not created by this transfer")
            fd=directories[ancestor]; leaf=parts[-1]
            if kind==b"5":
                private=".fube-data-child-"+secrets.token_hex(32)
                os.mkdir(private,0o700,dir_fd=fd)
                created=os.stat(private,dir_fd=fd,follow_symlinks=False)
                record={"path":entry,"kind":"directory","identity":[created.st_dev,created.st_ino],
                        "private":private}
                append_record(journal,record)
                child=hold(os.open(private,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd))
                require(identity(child)==record["identity"] and stat.S_ISDIR(created.st_mode),
                        "private upload child directory changed; retain")
                directories[entry]=child; records.append(record)
                publish(private,leaf,fd,fd)
                current=os.stat(leaf,dir_fd=fd,follow_symlinks=False)
                require([current.st_dev,current.st_ino]==record["identity"],
                        "upload child incarnation changed at publication; retain")
            else:
                file=hold(os.open(leaf,os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=fd))
                require(stat.S_ISREG(os.fstat(file).st_mode),"upload file is not regular")
                record={"path":entry,"kind":"file","identity":identity(file)}
                append_record(journal,record); records.append(record)
                digest=hashlib.sha256(); remaining=size
                while remaining:
                    chunk=read_exact(min(remaining,65536))
                    digest.update(chunk)
                    view=memoryview(chunk)
                    while view:
                        written=os.write(file,view)
                        require(written>0,"short upload write")
                        view=view[written:]
                    remaining-=len(chunk)
                os.fchmod(file,mode)
                files[entry]={"fd":file,"identity":record["identity"],"sha256":digest.hexdigest(),
                              "bytes":size,"mode":mode}
                require(not read_exact((-size)%512).strip(b"\0"),"invalid upload file padding")
            seen.add(entry)
        require(entries>0,"empty upload archive")
        verify(name)
        marker=hold(os.open(".fube-upload-marker",os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,
                            0o600,dir_fd=directory_fd))
        record={"path":".fube-upload-marker","kind":"file","identity":identity(marker)}
        append_record(journal,record); records.append(record)
        marker_item={"fd":marker,"identity":record["identity"],"sha256":hashlib.sha256(b"").hexdigest(),
                     "bytes":0,"mode":0o600}
        scoped="/proc/"+str(os.getpid())+"/fd/"+str(directory_fd)
        require(identity(directory_fd)==proof["directory"]
                and [os.stat(scoped).st_dev,os.stat(scoped).st_ino]==proof["directory"],
                "original descriptor-scoped upload source changed")
        def verify_namespace():
            verify(name)
            inspect_tree(records,directories)
        def verify_file(item):
            info=os.fstat(item["fd"])
            require(stat.S_ISREG(info.st_mode) and identity(item["fd"])==item["identity"]
                    and info.st_size==item["bytes"] and stat.S_IMODE(info.st_mode)==item["mode"]
                    and file_hash(item["fd"])==item["sha256"],
                    "original upload file identity/hash changed; retain")
        verify_namespace()
        for item in [*files.values(),marker_item]: verify_file(item)
        uploaded={}; markers={}
        targets=[(entry,item,uploaded) for entry,item in files.items()]
        targets.extend(((entry+"/" if entry else "")+"_.dir",marker_item,markers)
                       for entry in sorted(directories,key=lambda entry:len(entry.split("/")),reverse=True)
                       if entry)
        targets.append(("_.dir",marker_item,markers))
        for entry,item,receipts in targets:
            verify_namespace(); verify_file(item)
            source="/proc/"+str(os.getpid())+"/fd/"+str(item["fd"])
            info=os.stat(source)
            require([info.st_dev,info.st_ino]==item["identity"],"original upload file procfd changed")
            # The SDK sees only an ORIGINAL regular-file procfd, never a
            # directory or mutable child name. Do not use AzureCliProxy.sh.
            subprocess.run(["/opt/microsoft/servicefabric/bin/Fabric/Fabric.Code/azurecliproxy",
                "copyapplicationpackage","127.0.0.1:19000",source,"fabric:ImageStore",name+"/"+entry,""],
                check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                timeout=min(120,max(0.001,deadline-time.time())))
            verify_namespace(); verify_file(item)
            receipts[entry]={"source":source,"identity":item["identity"],"sha256":item["sha256"],
                            "bytes":item["bytes"]}
        verify_namespace()
        for item in [*files.values(),marker_item]: verify_file(item)
        return {"ownership":proof,"entries":entries,"bytes":total,"source":scoped,
                "image_store_path":name,"files":uploaded,"markers":markers,
                "upload_api":"native SDK ImageStore.UploadContent (azurecliproxy; per-file)"}
    finally:
        for fd in reversed(held): os.close(fd)
def remove_contents(detach):
    # The original owner inode is also the bounded creation journal. A copied
    # token or an unrecorded/replaced child never grants deletion authority.
    held=[]; directories={"":directory_fd}; records=[]
    try:
        journal=recorded_owner(); held.append(journal)
        data=os.pread(journal,65537,0)
        require(data[:64].decode("ascii")==proof["capability"] and len(data)<=65536,
                "upload inventory capability/byte bound")
        for line in data[64:].splitlines():
            if not line: continue
            require(len(records)<65,"upload cleanup inventory bound")
            record=json.loads(line)
            require(type(record) is dict and record.get("kind") in ("file","directory")
                    and set(record)==({"path","kind","identity","private"} if record["kind"]=="directory"
                                      else {"path","kind","identity"})
                    and isinstance(record["path"],str) and len(record["path"])<=99
                    and len(record["path"].split("/"))<=8
                    and all(re.fullmatch(r"[A-Za-z0-9_.-]+",part) and part not in (".","..")
                            for part in record["path"].split("/"))
                    and record["path"].split("/")[0]!=".fube-upload-owner"
                    and type(record["identity"]) is list and len(record["identity"])==2
                    and all(type(value) is int and value>=0 for value in record["identity"]),
                    "invalid upload cleanup inventory; retain")
            entry=record["path"]; ancestor,_,leaf=entry.rpartition("/")
            require(ancestor in directories and entry not in {r["path"] for r in records},
                    "invalid upload cleanup ancestor/duplicate; retain")
            fd=directories[ancestor]
            if record["kind"]=="directory":
                private=record["private"]
                require(isinstance(private,str) and re.fullmatch(r"\.fube-data-child-[a-f0-9]{64}",private),
                        "invalid private upload child; retain")
                try: info=os.stat(leaf,dir_fd=fd,follow_symlinks=False)
                except FileNotFoundError:
                    info=os.stat(private,dir_fd=fd,follow_symlinks=False)
                    record["path"]=ancestor+"/"+private if ancestor else private
                    entry=record["path"]; leaf=private
                require(stat.S_ISDIR(info.st_mode) and [info.st_dev,info.st_ino]==record["identity"],
                        "upload child incarnation changed; retain")
                child=os.open(leaf,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd); held.append(child)
                require(identity(child)==record["identity"],"upload child changed during cleanup capture; retain")
                directories[entry]=child
            records.append(record)
        inspect_tree(records,directories)
        detach()
        inspect_tree(records,directories)
        for record in reversed(records):
            ancestor,_,leaf=record["path"].rpartition("/")
            fd=directories[ancestor]
            info=os.stat(leaf,dir_fd=fd,follow_symlinks=False)
            require([info.st_dev,info.st_ino]==record["identity"],"upload child changed before removal; retain")
            if record["kind"]=="directory": os.rmdir(leaf,dir_fd=fd)
            else: os.unlink(leaf,dir_fd=fd)
    finally:
        for fd in reversed(held): os.close(fd)
try:
    if action=="reserve":
        require(type(proof) is dict and set(proof)=={"capability"}
                and re.fullmatch(r"[a-f0-9]{64}",proof["capability"]),"invalid upload creation capability")
        # The public/reusable name is never opened to mint ownership. Prepare
        # an unpublished private nonce in the same trusted parent, retaining
        # its original directory and owner descriptors through publication.
        private=".fube-data-reserve-"+secrets.token_hex(32)
        os.mkdir(private,0o700,dir_fd=parent_fd)
        private_created=True
        created=os.stat(private,dir_fd=parent_fd,follow_symlinks=False)
        private_identity=[created.st_dev,created.st_ino]
        directory_fd=os.open(private,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent_fd)
        require(stat.S_ISDIR(created.st_mode) and identity(directory_fd)==private_identity,
                "private upload directory changed before descriptor capture; retain")
        owner_fd=os.open(".fube-upload-owner",os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,
                         0o600,dir_fd=directory_fd)
        require(os.write(owner_fd,proof["capability"].encode("ascii"))==64,"short upload owner write")
        os.fsync(owner_fd)
        proof.update(parent=identity(parent_fd),directory=identity(directory_fd),
                     owner_file=identity(owner_fd))
        os.fsync(directory_fd)
        verify(private)
        publish(private,name)
        published=True
    else:
        require(action in ("verify","remove","transfer"),"invalid upload reservation action")
        directory_fd=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent_fd)
    verify(name)
    if action=="reserve":
        os.fsync(parent_fd)
    if action=="remove":
        # Detach the checked root from its reusable upload name. Check again
        # after rename, retaining the already checked child FD inventory.
        quarantine=".fube-data-remove-"+secrets.token_hex(32)
        detached=[]
        def detach():
            verify(name)
            os.rename(name,quarantine,src_dir_fd=parent_fd,dst_dir_fd=parent_fd)
            detached.append(True)
            verify(quarantine)
        try:
            remove_contents(detach)
            verify(quarantine)
            os.unlink(".fube-upload-owner",dir_fd=directory_fd)
            info=os.stat(quarantine,dir_fd=parent_fd,follow_symlinks=False)
            require([info.st_dev,info.st_ino]==proof["directory"],"detached upload directory changed")
            os.rmdir(quarantine,dir_fd=parent_fd)
        except Exception as error:
            raise RuntimeError("upload retained at "+parent+"/"+(quarantine if detached else name)+": "+str(error)) from error
        require(not os.path.lexists("/proc/self/fd/"+str(parent_fd)+"/"+quarantine),
                "upload reservation removal unconfirmed")
    print(json.dumps(transfer() if action=="transfer" else proof),flush=True)
except Exception as error:
    if action=="reserve" and private_created:
        if not published and directory_fd is not None and set(proof)=={"parent","directory","owner_file","capability"}:
            try:
                # Publication collisions grant no authority over the target.
                # Only the independently captured private original is ours.
                verify(private)
                os.unlink(".fube-upload-owner",dir_fd=directory_fd)
                info=os.stat(private,dir_fd=parent_fd,follow_symlinks=False)
                require([info.st_dev,info.st_ino]==proof["directory"],
                        "private upload directory changed; retain")
                os.rmdir(private,dir_fd=parent_fd)
            except Exception as cleanup_error:
                recovery_note(error,"private upload retained: "+str(cleanup_error))
        recovery_note(error,"original upload recovery (not target adoption): "+json.dumps({
            "private_path":parent+"/"+private,"private_identity":private_identity,
            "published_path":path,"proof":proof}))
    if action=="transfer":
        recovery_note(error,"original upload recovery (not target adoption): "+json.dumps({
            "published_path":path,"proof":proof}))
    raise
finally:
    if owner_fd is not None: os.close(owner_fd)
    if directory_fd is not None: os.close(directory_fd)
    os.close(parent_fd)
"""

STOP_UPLOAD_HELPER = r"""
import json,os,select,signal,sys
proof=json.loads(sys.argv[1])
try: fd=os.pidfd_open(proof["pid"])
except ProcessLookupError: sys.exit(0)
try:
    try:
        with open("/proc/"+str(proof["pid"])+"/stat") as status:
            start=status.read().rsplit(") ",1)[1].split()[19]
        with open("/proc/"+str(proof["pid"])+"/cmdline","rb") as command_file:
            command=command_file.read().split(b"\0")
    except FileNotFoundError: sys.exit(0)
    if start!=proof["start_time"] or (command!=[b""] and command[-2].decode()!=proof["token"]):
        raise RuntimeError("upload helper incarnation changed; refuse signal")
    if command!=[b""]: signal.pidfd_send_signal(fd,signal.SIGTERM)
    ready=select.poll(); ready.register(fd,select.POLLIN)
    if not ready.poll(10000): raise RuntimeError("original upload helper exit unconfirmed")
except ProcessLookupError: pass
finally: os.close(fd)
"""


@dataclass(frozen=True)
class UploadOwnership:
    path: str
    container_id: str
    parent: tuple[int, int]
    directory: tuple[int, int]
    owner_file: tuple[int, int]
    capability: str

    def evidence(self) -> dict:
        return {"parent": self.parent, "directory": self.directory,
                "owner_file": self.owner_file, "capability": self.capability}


def reserve_upload(allocation, run_id: str) -> UploadOwnership:
    require(re.fullmatch(r"[a-f0-9]{32}", run_id), "invalid upload run ID")
    path = "/var/lib/sf-node/fube-data-" + run_id
    capability = secrets.token_hex(32)
    cid = allocation.verify()
    value = strict_json(allocation.execute("python3", "-c", UPLOAD, path, "reserve",
                                          json.dumps({"capability": capability})))
    require(type(value) is dict and set(value) == {"parent", "directory", "owner_file", "capability"}
            and value["capability"] == capability
            and all(type(value[key]) is list and len(value[key]) == 2
                    and all(type(number) is int and number >= 0 for number in value[key])
                    for key in ("parent", "directory", "owner_file")), "invalid upload creation receipt")
    return UploadOwnership(path, cid, tuple(value["parent"]), tuple(value["directory"]),
                           tuple(value["owner_file"]), capability)


def check_upload(allocation, proof: UploadOwnership, action: str = "verify") -> None:
    require(allocation.verify() == proof.container_id, "original upload allocation changed")
    value = strict_json(allocation.execute("python3", "-c", UPLOAD, proof.path, action,
                                          json.dumps(proof.evidence())))
    require(value == json.loads(json.dumps(proof.evidence())), "upload reservation receipt changed")


def upload_archive(source: Path) -> tuple[bytes, int, int]:
    """Bounded, regular-file-only USTAR; never use tar's path-based extraction."""
    inventory = []
    for entry in source.rglob("*"):
        require(len(inventory) < UPLOAD_MAX_ENTRIES, "local upload archive entry bound")
        inventory.append(entry)
    output = io.BytesIO()
    total = 0
    with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for entry in sorted(inventory):
            relative = entry.relative_to(source).as_posix()
            require(len(relative) <= 98 and len(relative.split("/")) <= 8
                    and all(re.fullmatch(r"[A-Za-z0-9_.-]+", part) and part not in (".", "..")
                            for part in relative.split("/"))
                    and relative.split("/")[0] not in (".fube-upload-owner", ".fube-upload-marker")
                    and "_.dir" not in relative.split("/"),
                    "invalid local upload archive path")
            info = entry.lstat()
            header = tarfile.TarInfo(relative)
            if stat.S_ISDIR(info.st_mode):
                header.type, header.mode = tarfile.DIRTYPE, 0o700
                archive.addfile(header)
            else:
                require(stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) in (0o644, 0o755),
                        "local upload accepts regular package files/directories only")
                fd = os.open(entry, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(fd, "rb") as file:
                    opened = os.fstat(file.fileno())
                    require(stat.S_ISREG(opened.st_mode)
                            and (opened.st_dev, opened.st_ino) == (info.st_dev, info.st_ino)
                            and opened.st_size == info.st_size, "local upload file changed")
                    total += opened.st_size
                    require(total <= UPLOAD_MAX_BYTES, "local upload archive byte bound")
                    header.size, header.mode = opened.st_size, stat.S_IMODE(opened.st_mode)
                    archive.addfile(header, file)
    require(inventory, "empty local upload package")
    return output.getvalue(), len(inventory), total


def transfer_upload(allocation, proof: UploadOwnership, source: Path) -> dict:
    archive, entries, size = upload_archive(source)
    require(allocation.verify() == proof.container_id, "original upload allocation changed")
    token = secrets.token_hex(32)
    process = subprocess.Popen(
        ["docker", "exec", "-i", proof.container_id, "python3", "-c", UPLOAD,
         proof.path, "transfer", json.dumps(proof.evidence()), str(time.time() + UPLOAD_SECONDS), token],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=ROOT)
    stdout = b""
    try:
        stdout, stderr = process.communicate(archive, timeout=UPLOAD_SECONDS + 30)
        require(process.returncode == 0,
                f"descriptor-bound upload failed ({process.returncode}): {stderr[-6000:].decode(errors='replace')}")
        lines = stdout.splitlines()
        require(len(lines) == 2 and len(stdout) <= 65536, "invalid bounded upload helper receipt")
        helper, receipt = (strict_json(line.decode()) for line in lines)
        require(type(helper) is dict and set(helper) == {"pid", "start_time", "token"}
                and type(helper["pid"]) is int and helper["pid"] > 1
                and isinstance(helper["start_time"], str) and helper["start_time"].isdigit()
                and helper["token"] == token, "invalid upload helper process identity")
        require(type(receipt) is dict and set(receipt) == {
                    "ownership", "entries", "bytes", "source", "image_store_path", "upload_api",
                    "files", "markers"}
                and receipt["ownership"] == json.loads(json.dumps(proof.evidence()))
                and receipt["entries"] == entries and receipt["bytes"] == size
                and receipt["image_store_path"] == proof.path.rsplit("/", 1)[1]
                and receipt["upload_api"] == "native SDK ImageStore.UploadContent (azurecliproxy; per-file)"
                and isinstance(receipt["source"], str)
                and re.fullmatch(r"/proc/" + str(helper["pid"]) + r"/fd/[0-9]+", receipt["source"]),
                "original upload transfer receipt changed")
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as contents:
            expected, markers = {}, {}
            for entry in contents:
                if entry.isdir():
                    markers[entry.name.rstrip("/") + "/_.dir"] = (0, hashlib.sha256(b"").hexdigest())
                else:
                    digest = hashlib.sha256()
                    with contents.extractfile(entry) as file:
                        for chunk in iter(lambda: file.read(65536), b""):
                            digest.update(chunk)
                    expected[entry.name] = (entry.size, digest.hexdigest())
        markers["_.dir"] = (0, hashlib.sha256(b"").hexdigest())
        require(type(receipt["files"]) is dict and set(receipt["files"]) == set(expected),
                "original upload file receipt inventory changed")
        require(type(receipt["markers"]) is dict and set(receipt["markers"]) == set(markers),
                "original upload directory marker inventory changed")
        expected.update(markers)
        for name, item in {**receipt["files"], **receipt["markers"]}.items():
            require(type(item) is dict and set(item) == {"source", "identity", "bytes", "sha256"}
                    and type(item["identity"]) is list and len(item["identity"]) == 2
                    and all(type(value) is int and value >= 0 for value in item["identity"])
                    and (item["bytes"], item["sha256"]) == expected[name]
                    and isinstance(item["source"], str)
                    and re.fullmatch(r"/proc/" + str(helper["pid"]) + r"/fd/[0-9]+", item["source"]),
                    "original upload file source/identity/hash receipt changed")
        return {**receipt, "helper": helper, "container_id": proof.container_id,
                "copy_api": "docker exec -i python3; strict USTAR; retained original root FD, child/file FDs; "
                            "private directory capture/RENAME_NOREPLACE; file openat/O_EXCL",
                "note": "Transfer receipt only; not native deployment/scenario qualification evidence."}
    except BaseException as error:
        if isinstance(error, subprocess.TimeoutExpired):
            stdout = error.output or b""
        # Killing docker exec's client alone need not stop its container-side
        # process. Signal only the captured pidfd-bound incarnation; the remote
        # absolute alarm is a second bound, including before receipt delivery.
        captured = None
        try:
            first = stdout.splitlines()[0] if stdout else b""
            helper = strict_json(first.decode()) if first else None
            if (isinstance(helper, dict) and set(helper) == {"pid", "start_time", "token"}
                    and type(helper["pid"]) is int and helper["pid"] > 1 and helper["token"] == token
                    and isinstance(helper["start_time"], str) and helper["start_time"].isdigit()):
                captured = helper
                require(allocation.verify() == proof.container_id, "original upload allocation changed")
                allocation.execute("python3", "-c", STOP_UPLOAD_HELPER, json.dumps(helper), timeout=10)
        except Exception as stop_error:
            error.add_note("original upload helper stop unconfirmed (absolute deadline remains): " + str(stop_error))
        error.add_note("original upload helper recovery (not PID/name adoption): " + json.dumps({
            "container_id": proof.container_id, "helper": captured}))
        error.add_note("original upload reservation retained for cleanup/recovery: " + json.dumps(proof.evidence()))
        raise
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=10)


OWNERSHIP_READ = r"""
import json,sys,urllib.error,urllib.request
try:
    response=urllib.request.urlopen("http://127.0.0.1:19080"+sys.argv[1],timeout=10)
except urllib.error.HTTPError as error:
    response=error
with response:
    body=response.read(4194305)
    if len(body)>4194304: raise RuntimeError("native ownership response bound")
    print(json.dumps({"status":response.code,"body":json.loads(body) if body else None}))
"""


def ownership_read(allocation, path: str) -> dict | None:
    value = strict_json(allocation.execute("python3", "-c", OWNERSHIP_READ, path, timeout=15))
    require(type(value) is dict and set(value) == {"status", "body"}
            and type(value["status"]) is int, "invalid native ownership response")
    if value["status"] == 204:
        require(value["body"] is None, "native no-content response has a body")
        return None
    if value["status"] == 404:
        body = value["body"]
        # Native ImageStoreHandler::HandleListContentResult (h:129-183)
        # deliberately emits an empty 404 for a nonexistent nonempty path.
        # Executed against SF11.8.121.4; do NOT generalize to other routes.
        # A later provision still must yield positive native FileVersion proof.
        if body is None and re.fullmatch(
            r"/ImageStore/Store%2F[A-Za-z0-9][A-Za-z0-9_.-]{0,255}%2FApplicationManifest\."
            r"[A-Za-z0-9][A-Za-z0-9_.-]{0,255}\.xml\?api-version=6\.2", path
        ):
            return None
        require(type(body) is dict and type(body.get("Error")) is dict
                and body["Error"].get("Code") in {
                    "FABRIC_E_APPLICATION_NOT_FOUND", "FABRIC_E_APPLICATION_TYPE_NOT_FOUND"},
                "unrecognized native 404; absence is not established")
        return None
    require(value["status"] == 200 and type(value["body"]) is dict,
            f"native ownership query failed: HTTP {value['status']}")
    return value["body"]


@dataclass(frozen=True)
class TypeIdentity:
    name: str
    version: str
    capability: str
    manifest_sha256: str
    native_generation: tuple[int, int, int] | None = None


TYPE_IDENTITY_METHOD = {
    "api": "GET /ImageStore/{contentPath}?api-version=6.2",
    "content_path": "Store/{type}/ApplicationManifest.{version}.xml (provisioned, not upload staging)",
    "native_generation_fields": ["EpochDataLossNumber", "VersionNumber", "EpochConfigurationNumber"],
    "content_identity": "GetApplicationManifest canonical SHA-256 and run-minted type capability",
    "limitation": "Native file generation, not a ProvisionTimestamp or conditional lifecycle lock. "
                  "Actual native qualification remains pending; source/offline tests are not live evidence.",
}


@dataclass(frozen=True)
class ApplicationIdentity:
    name: str
    type_identity: TypeIdentity
    capability: str
    native_application_id: str


def manifest_identity(text: str) -> TypeIdentity:
    require(type(text) is str and len(text.encode()) <= MAX_RESPONSE, "native application manifest bound")
    root = ET.fromstring(text)
    require(root.tag == f"{{{runtime.NS}}}ApplicationManifest", "native application manifest schema")
    marker = root.find(f"{{{runtime.NS}}}Parameters/{{{runtime.NS}}}Parameter[@Name='FubeDataTypeCapability']")
    require(marker is not None and re.fullmatch(r"[a-f0-9]{64}", marker.get("DefaultValue", "")),
            "native type has no run-created immutable manifest capability")
    def canonical(element):
        children=list(element)
        # Native XSD defaults (ServiceFabricServiceModel.xsd:1449,1620,
        # 1634-1643) are omitted by the executed native XML serializer.
        defaults={
            f"{{{runtime.NS}}}Service":{"ServicePackageActivationMode":"SharedProcess"},
            f"{{{runtime.NS}}}StatelessService":{"InstanceCount":"1"},
            f"{{{runtime.NS}}}StatefulService":{"TargetReplicaSetSize":"1","MinReplicaSetSize":"1"},
        }.get(element.tag,{})
        attributes=[(name,value) for name,value in element.attrib.items() if defaults.get(name)!=value]
        if element.tag==f"{{{runtime.NS}}}Parameters":
            # Native GetApplicationManifest serializes this name-keyed map in
            # lexical order. The executed 11.8.121.4 response reverses our two
            # inserted capability declarations without changing either value.
            require(all(child.tag==f"{{{runtime.NS}}}Parameter" and child.get("Name") for child in children)
                    and len({child.get("Name") for child in children})==len(children),
                    "ambiguous native manifest parameter map")
            children.sort(key=lambda child:child.attrib["Name"])
        return [element.tag, sorted(attributes), (element.text or "").strip(),
                [canonical(child) for child in children]]
    fingerprint = hashlib.sha256(json.dumps(canonical(root), separators=(",", ":")).encode()).hexdigest()
    return TypeIdentity(root.attrib["ApplicationTypeName"], root.attrib["ApplicationTypeVersion"],
                        marker.attrib["DefaultValue"], fingerprint)


def type_store_path(expected: TypeIdentity) -> str:
    require(all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,255}", value)
                for value in (expected.name, expected.version)), "invalid native manifest store identity")
    return f"Store/{expected.name}/ApplicationManifest.{expected.version}.xml"


def native_type_generation(allocation, expected: TypeIdentity, *,
                           allow_absent: bool = False) -> tuple[int, int, int] | None:
    path = type_store_path(expected)
    value = ownership_read(allocation, "/ImageStore/" + quote(path, safe="") + "?api-version=6.2")
    if value is None:
        require(allow_absent, "positive native manifest generation unavailable; retain type")
        return None
    require(type(value) is dict and value.get("ContinuationToken") in (None, "")
            and type(value.get("StoreFiles")) is list
            and value.get("StoreFolders") in (None, []),
            "native provisioned manifest generation unavailable/ambiguous; retain type")
    if allow_absent and not value["StoreFiles"]:
        return None
    require(len(value["StoreFiles"]) == 1,
            "native provisioned manifest generation unavailable/ambiguous; retain type")
    item = value["StoreFiles"][0]
    require(type(item) is dict and type(item.get("StoreRelativePath")) is str
            and item["StoreRelativePath"].replace("\\", "/") == path,
            "native generation is not the actual provisioned application manifest; retain type")
    version = item.get("FileVersion")
    fields = TYPE_IDENTITY_METHOD["native_generation_fields"]
    require(type(version) is dict and set(version) == set(fields),
            "native provisioned manifest FileVersion unavailable/ambiguous; retain type")
    numbers = []
    for field in fields:
        number = version[field]
        require(type(number) is int or type(number) is str
                and re.fullmatch(r"-?(0|[1-9][0-9]{0,19})", number),
                "invalid native provisioned manifest generation; retain type")
        number = int(number)
        minimum, maximum = ((1, 2**64 - 1) if field == "VersionNumber" else
                            (0, 2**63 - 1) if field == "EpochDataLossNumber" else (-1, 2**63 - 1))
        require(minimum <= number <= maximum,
                "invalid native provisioned manifest generation range; retain type")
        numbers.append(number)
    return tuple(numbers)


def native_type_identity(allocation, expected: TypeIdentity, *, capture: bool = False) -> TypeIdentity | None:
    require(capture or expected.native_generation is not None,
            "no captured original native provision generation; refuse manifest-only adoption")
    require(not capture or expected.native_generation is None, "original type generation cannot be recaptured")
    path = (f"/ApplicationTypes/{quote(expected.name, safe='')}/$/GetApplicationManifest?"
            + urlencode({"api-version": "6.0", "ApplicationTypeVersion": expected.version}))
    generation = native_type_generation(allocation, expected) if capture else None
    value = ownership_read(allocation, path)
    if value is None:
        require(not capture, "native type absent during original generation capture; retain")
        return None
    actual = manifest_identity(value["Manifest"])
    require(actual == replace(expected, native_generation=None),
            "original native type manifest/capability changed; refuse unprovision; "
            + json.dumps({"expected":vars(replace(expected,native_generation=None)),
                          "actual":vars(actual),"observed_generation":generation,
                          "native_manifest":value["Manifest"][:16384]}))
    if not capture:
        generation = native_type_generation(allocation, expected)
    require(expected.native_generation in (None, generation),
            "original native type provisioning generation changed; refuse unprovision (even with identical manifest)")
    # Do not combine a manifest from one lifetime with a version observed
    # during replacement. Both native observations must remain stable.
    confirmed = ownership_read(allocation, path)
    require(confirmed is not None and manifest_identity(confirmed["Manifest"]) == actual
            and native_type_generation(allocation, expected) == generation,
            "native type manifest/provisioning generation changed during capture; retain type")
    return replace(actual, native_generation=generation)


APPLICATION_PACKAGE_ID = r"""
import json,os,stat,sys,xml.etree.ElementTree as ET
from pathlib import Path
work,application,type_name=sys.argv[1:]
def require(value,message):
    if not value: raise RuntimeError(message)
work=Path(work)
require(work.is_absolute() and not work.is_symlink() and work.is_dir(),
        "native deployed application work directory unavailable")
base=work.parent
require(not base.is_symlink(),"native application root is a symlink")
packages=[]
for path in base.glob("App.*.xml"):
    require(len(packages)<16,"native application package inventory bound")
    require(not path.is_symlink() and path.is_file() and stat.S_ISREG(path.stat().st_mode)
            and path.stat().st_size<=1048576,"native application package file bound")
    root=ET.fromstring(path.read_text())
    require(root.tag=="{http://schemas.microsoft.com/2011/01/fabric}ApplicationPackage",
            "native application package schema")
    require(root.get("NameUri")==application and root.get("ApplicationTypeName")==type_name,
            "native application package name/type drift")
    native_id=root.get("ApplicationId")
    require(type(native_id) is str and native_id and native_id==base.name,
            "native application instance ID/path mismatch")
    packages.append(native_id)
require(packages and len(set(packages))==1,"native original application instance not established")
print(json.dumps({"native_application_id":packages[0]}))
"""


def application_path(application: str) -> str:
    return "/Applications/" + quote(application.removeprefix("fabric:/").replace("/", "~"), safe="")


def native_application_identity(allocation, application: str, expected: TypeIdentity,
                                capability: str) -> ApplicationIdentity | None:
    value = ownership_read(allocation, application_path(application)
                           + "?api-version=6.0&ExcludeApplicationParameters=false")
    if value is None:
        return None
    require(value.get("Name") == application and value.get("TypeName") == expected.name
            and value.get("TypeVersion") == expected.version and value.get("Status") == "Ready",
            "native application type identity/status drift")
    parameters = value.get("Parameters")
    require(type(parameters) is list, "native application creation parameters unavailable")
    markers = [item["Value"] for item in parameters if item.get("Key") == "FubeDataCreationCapability"]
    require(markers == [capability], "native application creation capability mismatch")
    require(native_type_identity(allocation, expected) == expected, "native application type absent")
    ids = set()
    sid = application_path(application).removeprefix("/Applications/")
    for node in NODE_ENGINES:
        deployed = ownership_read(allocation,
            f"/Nodes/{node}/$/GetApplications/{sid}?api-version=6.0")
        if deployed is None:
            continue
        # Native DeployedApplicationQueryResult exposes no TypeVersion. Version
        # is established by the application query and immutable type manifest.
        require(deployed.get("Name") == application and deployed.get("TypeName") == expected.name
                and deployed.get("Status") == "Active",
                "native deployed application identity/status ambiguous")
        captured = strict_json(allocation.execute("python3", "-c", APPLICATION_PACKAGE_ID,
            deployed["WorkDirectory"], application, expected.name, timeout=15))
        require(type(captured) is dict and set(captured) == {"native_application_id"}
                and type(captured["native_application_id"]) is str and captured["native_application_id"],
                "native application instance ID missing")
        ids.add(captured["native_application_id"])
    require(len(ids) == 1, "original native application instance is unavailable/ambiguous; retain")
    return ApplicationIdentity(application, expected, capability, ids.pop())


def deployment_mutation(allocation, outcomes: dict, stage: str, *args: str) -> None:
    outcomes[stage] = "ambiguous"
    try:
        allocation.sf("application", *args)
    except Exception as error:
        # Only explicit definitive rejection is classified here. Neither this
        # classification nor a possibly accepted timeout creates a capability.
        if re.search(r"Application(?:Type)?AlreadyExists|FABRIC_E_APPLICATION(?:_TYPE)?_ALREADY_EXISTS"
                     r"|already[ _-]+exists|collision", str(error), re.IGNORECASE):
            outcomes[stage] = "rejected"
        raise
    outcomes[stage] = "accepted"


def verify_image_store_upload_absent(allocation, upload: UploadOwnership) -> dict:
    """Read-only proof for the native upload, separately from local staging."""
    require(allocation.verify()==upload.container_id,"original upload allocation changed")
    name=upload.path.rsplit("/",1)[1]
    require(re.fullmatch(r"fube-data-[a-f0-9]{32}",name),"invalid original ImageStore upload path")
    path="/ImageStore/"+name+"?api-version=6.2"
    response=strict_json(allocation.execute("python3","-c",OWNERSHIP_READ,path,timeout=15))
    require(response=={"status":404,"body":None},
            "native ImageStore upload absence not proven; retain recovery path, never delete by name")
    return {"path":name,"native_response":response,"absence_verified":True}


def cleanup_data_deployment(allocation, result: dict, outcomes: dict,
                            original_type: TypeIdentity | None,
                            original_application: ApplicationIdentity | None,
                            upload: UploadOwnership | None) -> None:
    cleanup = result["cleanup"]
    application_clear = outcomes["create"] == "not_requested"
    def retain(kind, reason):
        cleanup[kind] = "retained: " + reason
        result["errors"].append(f"{kind} cleanup: {reason}")
        cleanup["retained_for_recovery"] = {
            "application": result["application"], "type": result["type_name"], "version": result["version"],
            "local_package_path": result.get("local_package_path"),
            "request_correlation_only": result.get("deployment_requests"),
            "outcomes": dict(outcomes), "original_native_application_id":
                original_application.native_application_id if original_application else None,
            "original_type_manifest_sha256": original_type.manifest_sha256 if original_type else None,
            "original_native_type_generation": original_type.native_generation if original_type else None,
            "native_type_identity_method": TYPE_IDENTITY_METHOD,
            "original_upload": ({"path": upload.path, "container_id": upload.container_id,
                                 **upload.evidence()} if upload else None),
            "reason": "No name/token/content-only adoption. Inspect original native generation/instance "
                      "and request outcomes before manual recovery; missing metadata grants no authority.",
        }
    try:
        require(original_type is None or outcomes["provision"] == "accepted",
                "rejected/unconfirmed provision cannot grant original type cleanup authority")
        require(original_application is None or outcomes["create"] == "accepted",
                "rejected/unconfirmed create cannot grant original application cleanup authority")
        require(original_type is None or original_type.native_generation is not None,
                "original native type provisioning generation unavailable; refuse manifest-only cleanup")
        if original_application is not None:
            current = native_application_identity(allocation, original_application.name,
                original_application.type_identity, original_application.capability)
            require(current is None or current == original_application,
                    "original native application instance changed; refuse deletion (even with a copied capability)")
            if current is not None:
                allocation.sf("application", "delete", "--application-id",
                              original_application.name.removeprefix("fabric:/"))
                runtime.wait_condition(lambda: ownership_read(allocation,
                    application_path(original_application.name) + "?api-version=6.0") is None,
                    "original application deletion/absence (no retry against replacements)", timeout=60)
                cleanup["application"] = "deleted; absence confirmed"
            else:
                cleanup["application"] = "absence confirmed"
            application_clear = True
        elif not application_clear:
            retain("application", f"{outcomes['create']} create has no captured original native instance; "
                   "a matching name/type/parameter is not creation proof; late completion possible")
        if original_type is not None:
            if not application_clear:
                retain("type", "application identity unresolved; original type retained")
            else:
                current = native_type_identity(allocation, original_type)
                if current is not None:
                    allocation.sf("application", "unprovision", "--application-type-name", original_type.name,
                                  "--application-type-version", original_type.version)
                    runtime.wait_condition(lambda: native_type_identity(allocation, original_type) is None,
                                           "original type manifest absence", timeout=60)
                    cleanup["type"] = "unprovisioned; absence confirmed"
                else:
                    cleanup["type"] = "absence confirmed"
        elif outcomes["provision"] != "not_requested":
            retain("type", f"{outcomes['provision']} provision has no captured original native manifest/generation; "
                   "matching names/versions/content/parameters grant no unprovision authority; late completion possible")
    except Exception as error:
        retain("deployment", str(error))
    finally:
        if upload is not None:
            try:
                cleanup["image_store_upload"]=verify_image_store_upload_absent(allocation,upload)
            except Exception as error:
                retain("image_store_upload",str(error))
            try:
                check_upload(allocation, upload, "remove")
                cleanup["upload"] = "original reservation removed"
            except Exception as error:
                retain("upload", str(error))


@dataclass
class DeploymentVersion:
    source: Path
    expected: TypeIdentity
    outcomes: dict
    original: TypeIdentity | None = None
    upload: UploadOwnership | None = None

    def evidence(self) -> dict:
        return {
            "local_package_path": str(self.source), "requested_type": vars(self.expected),
            "original_type": vars(self.original) if self.original else None,
            "outcomes": dict(self.outcomes),
            "upload": ({"path": self.upload.path, "container_id": self.upload.container_id,
                        **self.upload.evidence()} if self.upload else None),
        }


class NativeDataDeploymentPolicy:
    """Original native identities and pinned uploads for the preserved runtime suite."""

    def __init__(self, bundle: Bundle):
        self.bundle = bundle
        self.type_capability = secrets.token_hex(32)
        self.creation_capability = secrets.token_hex(32)
        self.versions: dict[str, DeploymentVersion] = {}
        self.outcomes = {"create": "not_requested"}
        self.original_application: ApplicationIdentity | None = None
        self.allocation_identity: tuple[str, str, str] | None = None
        self.cleanup_complete = False

    def verify_allocation(self, allocation) -> None:
        require(self.allocation_identity == (allocation.name, allocation.owner, allocation.container_id)
                and allocation.verify() == self.allocation_identity[2],
                "legacy deployment original allocation changed; retain all capabilities")

    def parameters(self) -> str:
        return json.dumps({"FubeDataTypeCapability": self.type_capability,
                           "FubeDataCreationCapability": self.creation_capability})

    def prepare(self, binaries: runtime.Binaries, result: dict, *, upgrade: bool) -> DeploymentVersion:
        require(binaries == self.bundle.legacy and binaries.template_root is not None,
                "legacy deployment requires the verified nested v3 bundle; no build fallback")
        number = 2 if upgrade else 1
        for name in runtime.PACKAGE_FILES:
            self.bundle.artifact(f"legacy/package-v{number}/{name}")
        self.bundle.artifact("legacy/rust-runtime-app")
        self.bundle.artifact("legacy/runtime_observer")
        source, type_name, version = runtime.package(
            result["run_id"], binaries.app, templates=binaries.template_root, upgrade=upgrade)
        require(type_name == result["type_name"] and version == ("1.2" if upgrade else "1.1")
                and version not in self.versions, "legacy minted package version/type drift")
        self.bundle.artifact("legacy/rust-runtime-app", source / "RustRuntimeService/Code/rust-runtime-app")
        path = source / "ApplicationManifest.xml"
        manifest = ET.parse(path)
        root = manifest.getroot()
        containers = root.findall(f"{{{runtime.NS}}}Parameters")
        require(len(containers) <= 1, "ambiguous legacy ownership parameter container")
        if containers:
            parameters = containers[0]
        else:
            parameters = ET.Element(f"{{{runtime.NS}}}Parameters")
            root.insert(0, parameters)
        for name, capability in json.loads(self.parameters()).items():
            matches = parameters.findall(f"{{{runtime.NS}}}Parameter[@Name='{name}']")
            require(len(matches) <= 1 and (not matches or matches[0].get("DefaultValue") == ""),
                    "legacy ownership templates must be empty; never reuse stored capabilities")
            marker = matches[0] if matches else ET.SubElement(
                parameters, f"{{{runtime.NS}}}Parameter", Name=name)
            marker.set("DefaultValue", capability)
        manifest.write(path, encoding="utf-8", xml_declaration=True)
        record = DeploymentVersion(source, manifest_identity(path.read_text()),
                                   {"provision": "not_requested"})
        self.versions[version] = record
        result.setdefault("local_package_paths", {})[version] = str(source)
        return record

    def provision(self, allocation, record: DeploymentVersion, result: dict) -> None:
        self.verify_allocation(allocation)
        require(native_type_generation(allocation, record.expected, allow_absent=True) is None,
                "provisioned ImageStore manifest path collision; no unprovision authority")
        record.upload = reserve_upload(allocation, uuid.uuid4().hex)
        result.setdefault("upload_paths", []).append(record.upload.path)
        check_upload(allocation, record.upload)
        result.setdefault("upload_transfers", {})[record.expected.version] = transfer_upload(
            allocation, record.upload, record.source)
        deployment_mutation(allocation, record.outcomes, "provision", "provision",
                            "--application-type-build-path", record.upload.path.rsplit("/", 1)[1])
        record.original = native_type_identity(allocation, record.expected, capture=True)
        require(record.original is not None, "accepted legacy provision has no original native manifest/generation")
        result["deployment_versions"] = {version: value.evidence() for version, value in self.versions.items()}

    def deploy_initial(self, allocation, binaries: runtime.Binaries, result: dict) -> None:
        require(self.allocation_identity is None, "legacy deployment policy cannot be reused")
        self.allocation_identity = (allocation.name, allocation.owner, allocation.container_id)
        self.verify_allocation(allocation)
        result["native_type_identity_method"] = TYPE_IDENTITY_METHOD
        record = self.prepare(binaries, result, upgrade=False)
        allocation.sf("cluster", "select", "--endpoint", "http://127.0.0.1:19080")
        require(not any(item["name"] == result["application"] for item in
                        runtime.items(allocation.sf("application", "list", timeout=10))),
                "minted legacy application collision; no adoption/deletion capability granted")
        require(not any(item["name"] == record.expected.name for item in
                        runtime.items(allocation.sf("application", "type-list", timeout=10))),
                "minted legacy type collision; no adoption/unprovision capability granted")
        self.provision(allocation, record, result)
        deployment_mutation(allocation, self.outcomes, "create", "create",
                            "--app-name", result["application"], "--app-type", record.expected.name,
                            "--app-version", record.expected.version, "--parameters", self.parameters())
        self.original_application = runtime.wait_condition(lambda: native_application_identity(
            allocation, result["application"], record.original, self.creation_capability),
            "initial legacy native application instance/capability", timeout=60)
        result["original_application_identity"] = {
            "native_application_id": self.original_application.native_application_id,
            "creation_capability": self.original_application.capability,
        }

    def current_application(self, allocation) -> ApplicationIdentity | None:
        original = self.original_application
        require(original is not None and self.outcomes["create"] == "accepted",
                "legacy create has no captured original native application; never adopt at cleanup")
        value = ownership_read(allocation, application_path(original.name) + "?api-version=6.0")
        if value is None:
            return None
        record = self.versions.get(value.get("TypeVersion"))
        require(record is not None and record.original is not None
                and record.outcomes["provision"] == "accepted",
                "legacy application version drift/unconfirmed provisioning; retain application and types")
        current = native_application_identity(allocation, original.name, record.original, original.capability)
        require(current is not None and current == replace(original, type_identity=record.original),
                "original legacy native application instance changed; refuse copied-capability replacement")
        return current

    def provision_upgrade(self, allocation, binaries: runtime.Binaries, result: dict) -> tuple[str, str]:
        self.verify_allocation(allocation)
        require(self.current_application(allocation) == self.original_application,
                "legacy application changed before config-only upgrade")
        record = self.prepare(binaries, result, upgrade=True)
        require(not any(item["name"] == record.expected.name and item["version"] == record.expected.version
                        for item in runtime.items(allocation.sf("application", "type-list", timeout=10))),
                "legacy upgrade type collision; no adoption/unprovision capability granted")
        self.provision(allocation, record, result)
        return record.expected.version, self.parameters()

    def confirm_upgrade(self, allocation, result: dict, version: str) -> None:
        self.verify_allocation(allocation)
        current = self.current_application(allocation)
        require(current is not None and current.type_identity.version == version,
                "native config-only upgrade did not retain the original application at the requested version")
        result["configuration_upgrade"]["native_application_identity"] = {
            "native_application_id": current.native_application_id, "version": version,
            "creation_capability": current.capability,
        }

    def require_type_absent(self, allocation, record: DeploymentVersion) -> bool:
        expected = record.original
        require(expected is not None, "no original native type generation; retain")
        require(native_type_identity(allocation, expected) is None
                and native_type_generation(allocation, expected, allow_absent=True) is None
                and not any(item["name"] == expected.name and item["version"] == expected.version for item in
                            runtime.items(allocation.sf("application", "type-list", timeout=10))),
                "legacy original type absence not independently confirmed")
        return True

    def cleanup(self, allocation, result: dict) -> None:
        cleanup = result["cleanup"]
        absence = {"application": False, "types": {}, "uploads": {}}
        cleanup["independent_absence"] = absence
        def retain(kind, reason):
            cleanup.setdefault("retained", {})[kind] = str(reason)
            result["errors"].append(f"legacy {kind} cleanup: {reason}")
        try:
            if self.allocation_identity is not None:
                self.verify_allocation(allocation)
            for record in self.versions.values():
                if record.original is not None:
                    require(record.outcomes["provision"] == "accepted",
                            "unconfirmed/rejected provision cannot grant legacy cleanup authority")
                    if native_type_identity(allocation, record.original) is None:
                        self.require_type_absent(allocation, record)
            application_clear = self.outcomes["create"] == "not_requested"
            if self.original_application is not None:
                current = self.current_application(allocation)
                if current is not None:
                    allocation.sf("application", "delete", "--application-id",
                                  current.name.removeprefix("fabric:/"))
                    runtime.wait_condition(lambda: ownership_read(allocation,
                        application_path(current.name) + "?api-version=6.0") is None,
                        "original legacy application absence (no replacement retries)", timeout=60)
                cleanup["application"] = "deleted; absence confirmed" if current else "absence confirmed"
                absence["application"] = application_clear = True
            elif not application_clear:
                retain("application", f"{self.outcomes['create']} create has no captured original native instance; "
                       "late completion possible; matching names/parameters grant no deletion authority")
            for version, record in self.versions.items():
                try:
                    if record.outcomes["provision"] == "not_requested":
                        continue
                    require(record.original is not None,
                            f"{record.outcomes['provision']} provision has no captured original manifest/generation; "
                            "late completion possible; no adoption from later queries")
                    require(application_clear, "legacy application identity unresolved; original type retained")
                    if native_type_identity(allocation, record.original) is not None:
                        allocation.sf("application", "unprovision", "--application-type-name", record.original.name,
                                      "--application-type-version", version)
                    runtime.wait_condition(lambda: self.require_type_absent(allocation, record),
                                           "original legacy type absence in independent native queries", timeout=60)
                    cleanup.setdefault("types", {})[version] = "unprovisioned; absence confirmed"
                    absence["types"][version] = True
                except Exception as error:
                    retain("type " + version, error)
        except Exception as error:
            retain("deployment", error)
        finally:
            for version, record in self.versions.items():
                if record.upload is not None:
                    try:
                        cleanup.setdefault("image_store_uploads",{})[version]=verify_image_store_upload_absent(
                            allocation,record.upload)
                    except Exception as error:
                        retain("image_store_upload "+version,error)
                    try:
                        check_upload(allocation, record.upload, "remove")
                        cleanup.setdefault("uploads", {})[version] = "original reservation removed; absence confirmed"
                        absence["uploads"][version] = True
                    except Exception as error:
                        retain("upload " + version, error)
            result["deployment_outcomes"] = dict(self.outcomes)
            result["deployment_versions"] = {version: record.evidence() for version, record in self.versions.items()}
            if cleanup.get("retained"):
                cleanup["retained_for_recovery"] = {
                    "application": result["application"], "original_allocation": self.allocation_identity,
                    "original_native_application_id": (self.original_application.native_application_id
                                                       if self.original_application else None),
                    "application_creation_capability": self.creation_capability,
                    "versions": result["deployment_versions"], "outcomes": dict(self.outcomes),
                    "native_type_identity_method": TYPE_IDENTITY_METHOD, "reasons": dict(cleanup["retained"]),
                    "note": "No name/token/content-only adoption; missing original native proof grants no authority.",
                }
            self.cleanup_complete = (not cleanup.get("retained") and absence["application"]
                and set(absence["types"]) == set(absence["uploads"]) == set(self.versions) == {"1.1", "1.2"})
            cleanup["safe_owner_cleanup"] = self.cleanup_complete


def legacy_full(allocation, bundle: Bundle, result: dict) -> None:
    evidence = result["scenarios"]["legacy_full"]
    evidence.update(status="FAIL", started_at=time.time())
    evidence.pop("reason", None)
    policy = NativeDataDeploymentPolicy(bundle)
    try:
        qualified = runtime.qualify(allocation, full=True, binaries=bundle.legacy, stateful="three",
                                    deployment_policy=policy)
        evidence["qualification"] = qualified
        require(qualified["status"] == "PASS_FULL_EMPTY_STATEFUL_CONTROL_PLANE_AND_STATELESS_PROFILE"
                and not qualified["errors"], "preserved native full empty-state/stateless qualification failed")
        require(policy.cleanup_complete and qualified["cleanup"].get("safe_owner_cleanup") is True,
                "legacy full native qualification lacks original-owner cleanup/independent absence evidence")
        evidence["status"] = "PASS"
    except Exception as error:
        evidence["error"] = f"{type(error).__name__}: {error}"
        result["errors"].append("legacy_full: " + evidence["error"])
    finally:
        evidence["finished_at"] = time.time()


def qualify(allocation, bundle: Bundle, profile: str) -> dict:
    run_id = uuid.uuid4().hex
    result = {
        "run_id": run_id, "application": f"fabric:/FubeData{run_id}",
        "type_name": f"FubeData{run_id}Type", "version": "1.1",
        "onebox": allocation.name, "owner": allocation.owner, "container_id": allocation.container_id,
        "sf_version": "11.8.121.4", "profile": profile, "status": "FAIL", "errors": [], "cleanup": {},
        "native_type_identity_method": TYPE_IDENTITY_METHOD,
        "scenarios": {name: {"status": "MISSING", "reason": "not executed"} for name in required_scenarios(profile)},
    }
    outcomes = {"provision": "not_requested", "create": "not_requested"}
    original_type, original_application, upload = None, None, None
    qualification = None
    try:
        result["provenance"] = provenance(bundle)
        result["bundle"] = bundle.metadata
        source, type_name, version = package(bundle, run_id, profile != "rust")
        result.update(type_name=type_name, version=version)
        result["local_package_path"] = str(source)
        expected_type = manifest_identity((source / "ApplicationManifest.xml").read_text())
        require(expected_type.name == type_name and expected_type.version == version, "local type identity drift")
        creation_capability = secrets.token_hex(32)
        result["deployment_requests"] = {
            "type": vars(expected_type), "application_creation_capability": creation_capability,
            "note": "Requested values are recovery/correlation labels, never original-instance cleanup proof.",
        }
        allocation.sf("cluster", "select", "--endpoint", "http://127.0.0.1:19080")
        require(not any(item["name"] == result["application"] for item in
                        runtime.items(allocation.sf("application", "list", timeout=10))),
                "minted application collision; no adoption/deletion capability granted")
        require(not any(item["name"] == type_name for item in
                        runtime.items(allocation.sf("application", "type-list", timeout=10))),
                "minted application type collision; no adoption/deletion capability granted")
        require(native_type_generation(allocation, expected_type, allow_absent=True) is None,
                "provisioned ImageStore manifest path collision; no adoption/unprovision capability granted")
        result["upload_reservation_request"] = {
            "path": "/var/lib/sf-node/fube-data-" + run_id,
            "status": "no cleanup authority until exclusive creation receipt is captured",
        }
        upload = reserve_upload(allocation, run_id)
        result["upload_paths"] = [upload.path]
        result["upload_reservation"] = {"path": upload.path, "container_id": upload.container_id,
                                       **upload.evidence()}
        check_upload(allocation, upload)
        result["upload_transfer"] = transfer_upload(allocation, upload, source)
        deployment_mutation(allocation, outcomes, "provision", "provision",
                            "--application-type-build-path", upload.path.rsplit("/", 1)[1])
        original_type = native_type_identity(allocation, expected_type, capture=True)
        require(original_type is not None, "accepted provision has no original native manifest/generation identity")
        result["original_type_identity"] = vars(original_type)
        deployment_mutation(allocation, outcomes, "create", "create", "--app-name", result["application"],
                            "--app-type", type_name, "--app-version", version, "--parameters",
                            json.dumps({"FubeDataCreationCapability": creation_capability}))
        nodes = MIXED_NODES if profile != "rust" else tuple(NODE_ENGINES)
        result["data_service_create"] = create_data_service(
            allocation, result["application"], result["application"] + "/Data", nodes)
        original_application = runtime.wait_condition(lambda: native_application_identity(
            allocation, result["application"], original_type, creation_capability),
            "initial native application instance/capability", timeout=60)
        result["original_application_identity"] = {
            "native_application_id": original_application.native_application_id,
            "creation_capability": original_application.capability,
            "note": "Captured after accepted creation, before scenarios; the REST Id is only an encoded name.",
        }
        qualification = Qualification(allocation, bundle, result, profile != "rust", original_application)
        qualification.run()
    except (Exception, KeyboardInterrupt) as error:
        result["errors"].append(f"qualification: {type(error).__name__}: {error}")
        if getattr(error, "__notes__", None):
            result["qualification_recovery_notes"] = list(error.__notes__)
    finally:
        if qualification is not None:
            result["cleanup"]["resume"] = qualification.resume()
            result["cleanup"]["hooks"] = qualification.reset_hooks()
            result["cleanup"]["auxiliaries"] = qualification.cleanup_auxiliaries()
            if qualification.paused:
                result["errors"].append("owned process resume not confirmed")
            if qualification.hooks:
                # Departed unit hooks no longer exist, but absence must be queried,
                # never presumed merely from an HTTP error.
                try:
                    current = native_snapshot(bundle, qualification.service)
                    endpoints = {replica["address"] for replica in current["replicas"]}
                    departed = {}
                    for endpoint, _ in qualification.hooks:
                        if endpoint not in endpoints:
                            original = qualification.hook_identities.get(endpoint)
                            require(original is not None, "original hook owner was not captured")
                            departed[endpoint] = runtime.wait_native_replica_absent(
                                bundle.observer, original["service"], original["node"], original["replica"])
                    result["cleanup"]["departed_hook_endpoints"] = departed
                    qualification.hooks = {(endpoint, name) for endpoint, name in qualification.hooks
                                           if endpoint not in departed}
                except Exception as error:
                    result["errors"].append("hook departure observation: " + str(error))
                if qualification.hooks:
                    result["errors"].append("qualification hook reset not confirmed")
        if result["errors"] or any(scenario["status"] != "PASS" for scenario in result["scenarios"].values()):
            try:
                result["failure_artifacts"] = runtime.failure_artifacts(allocation, result["type_name"])
            except Exception as error:
                result["diagnostic_error"] = str(error)
        result["deployment_outcomes"] = outcomes
        pending_fas = [auxiliary["fas"] for auxiliary in qualification.auxiliaries
                       if auxiliary.get("fas") and not auxiliary["fas"].get("native_cleanup_terminal")] if qualification else []
        if pending_fas:
            result["errors"].append("owned native FAS operation/transport rollback remains unconfirmed; original deployment retained")
            result["cleanup"]["pending_native_fas"] = pending_fas
        else:
            cleanup_data_deployment(allocation, result, outcomes, original_type, original_application, upload)
            if qualification is not None:
                try:
                    qualification.confirm_auxiliaries_absent()
                except Exception as error:
                    result["errors"].append("auxiliary native absence/original-route retirement: " + str(error))
        if profile == "full":
            legacy_full(allocation, bundle, result)
    return finalize(result)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", nargs="?")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--build-bundle", type=Path, help="upstream-only, NEW directory, clean source")
    mode.add_argument("--prebuilt", type=Path, help="strict bundle; no Cargo or compiler fallback")
    parser.add_argument("--profile", choices=["rust", "mixed", "full"])
    parser.add_argument("--existing-owner", help="exact caller-owned allocation capability; retain allocation")
    parser.add_argument("--report", type=Path, help="NEW evidence file, never overwrite")
    args = parser.parse_args()
    if sys.platform != "linux" or os.uname().machine != "x86_64":
        parser.error("data/native qualification requires Linux x86-64")
    if args.build_bundle is not None:
        if args.name or args.existing_owner or args.profile or args.report:
            parser.error("--build-bundle is not qualification; do not combine options")
        build_bundle(args.build_bundle)
        print(json.dumps({"status": "BUILT", "bundle": str(args.build_bundle)}))
        return
    if not args.name or not re.fullmatch(r"[a-z][a-z0-9-]{0,39}", args.name) or not args.profile or not args.report:
        parser.error("qualification requires a unique name, --profile and --report NEWFILE")
    if args.existing_owner is not None and not re.fullmatch(r"[a-f0-9]{32}", args.existing_owner):
        parser.error("--existing-owner must be the exact original owner capability")
    result = {
        "status": "FAIL", "onebox": args.name, "profile": args.profile, "errors": [], "cleanup": {},
        "scenarios": {name: {"status": "MISSING", "reason": "not executed"}
                      for name in required_scenarios(args.profile)},
    }
    created, owner, report_file = False, None, None
    def allocated(value):
        nonlocal created, owner
        require(not created and owner is None and isinstance(value, str)
                and re.fullmatch(r"[a-f0-9]{32}", value), "immutable original creation capability")
        owner, created = value, True
        result["original_allocation_owner"] = owner
    try:
        # Refuse report collisions and invalid bundle inputs before allocation.
        report_file = args.report.open("x", encoding="utf-8")
        bundle = load_bundle(args.prebuilt)
        if args.existing_owner is not None:
            owner = args.existing_owner
        else:
            returned_owner = runtime.create_owned(args.name, on_allocated=allocated)
            require(created and returned_owner == owner, "create returned a different/unpublished owner capability")
        allocation = runtime.Allocation(args.name, owner)
        result = qualify(allocation, bundle, args.profile)
    except (Exception, KeyboardInterrupt) as error:
        result["errors"].append(f"execution: {type(error).__name__}: {error}")
    finally:
        if created:
            result["original_allocation_owner"] = owner
            try:
                require(owner is not None, "original allocation capability missing; retain for recovery")
                result["cleanup"]["onebox"] = runtime.destroy_owned(args.name, owner)
            except Exception as error:
                result["errors"].append("onebox cleanup: " + str(error))
                result["cleanup"]["onebox"] = "unconfirmed; original owned state retained for recovery"
        else:
            result["cleanup"]["onebox"] = ("caller-owned allocation retained" if args.existing_owner
                                           else "create rejected; no cleanup capability")
        finalize(result)
        if report_file is not None:
            try:
                json.dump(result, report_file, indent=2)
                report_file.flush()
                os.fsync(report_file.fileno())
            except Exception as error:
                result["errors"].append("evidence file: " + str(error))
                result["status"] = "FAIL"
            finally:
                report_file.close()
    print(json.dumps(result, indent=2))
    if result["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
