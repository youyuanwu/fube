#!/usr/bin/env python3
"""Native application-runtime qualification, not the standalone Rust test host.

Default: create/qualify/destroy one exact owned onebox. --existing-owner is an
explicit capability for a caller-owned allocation; it never destroys that box.
Only normal management deployment is performed through sfctl. All application
hosting/lifecycle is Rust IPC; resolution is independently mssf-client.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import concurrent.futures
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET
if sys.platform == "linux":
    import onebox_ctl
else:
    onebox_ctl = None

ROOT = Path(__file__).resolve().parents[1]
OWNER_LABEL = "io.github.service-fabric-apps.cluster-owner"
NS = "http://schemas.microsoft.com/2011/01/fabric"
ET.register_namespace("", NS)
DEFAULT_APP = ROOT / "target/debug/rust-runtime-app"
DEFAULT_OBSERVER = ROOT / "target/debug/examples/runtime_observer"


def run(*args: str, timeout: float = 120) -> str:
    result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"{args[0]} failed ({result.returncode}): {result.stderr[-6000:]}")
    return result.stdout


@dataclass(frozen=True)
class Binaries:
    app: Path
    observer: Path
    dependency_graph: str
    template_root: Path | None = None
    bundle_manifest_sha256: str | None = None


PACKAGE_FILES = (
    "ApplicationManifest.xml", "RustRuntimeService/ServiceManifest.xml",
    "RustRuntimeService/Config/Settings.xml", "RustRuntimeService/Data/marker.txt",
)


def upgrade_template(directory: Path) -> None:
    path = directory / "ApplicationManifest.xml"
    manifest = ET.parse(path)
    manifest.getroot().set("ApplicationTypeVersion", "1.2")
    manifest.find(f"{{{NS}}}ServiceManifestImport/{{{NS}}}ServiceManifestRef").set("ServiceManifestVersion", "1.2")
    manifest.write(path, encoding="utf-8", xml_declaration=True)
    path = directory / "RustRuntimeService/ServiceManifest.xml"
    manifest = ET.parse(path)
    manifest.getroot().set("Version", "1.2")
    manifest.find(f"{{{NS}}}ConfigPackage").set("Version", "2.0")
    manifest.write(path, encoding="utf-8", xml_declaration=True)
    path = directory / "RustRuntimeService/Config/Settings.xml"
    settings = ET.parse(path)
    settings.find(f"{{{NS}}}Section/{{{NS}}}Parameter[@Name='Value']").set("Value", "after-upgrade")
    settings.write(path, encoding="utf-8", xml_declaration=True)


def validate_dependency_graph(graph: str) -> None:
    if not isinstance(graph, str) or not graph.strip():
        raise RuntimeError("missing application dependency graph")
    if any(name in graph.lower() for name in ("mssf-core", "mssf-com", "fabricruntime", "fabriccommon")):
        raise RuntimeError("application dependency graph contains native SF adapters")


def digest(path: Path) -> str:
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def build_binaries() -> Binaries:
    run("cargo", "build", "--locked", "-p", "rust-runtime-app")
    run("cargo", "build", "--locked", "-p", "mssf-client", "--example", "runtime_observer")
    graph = run("cargo", "tree", "--locked", "-p", "rust-runtime-app")
    validate_dependency_graph(graph)
    return Binaries(DEFAULT_APP, DEFAULT_OBSERVER, graph)


def build_bundle(directory: Path) -> None:
    binaries = build_binaries()
    directory.mkdir(parents=True, exist_ok=False)
    hashes = {}
    for source, name in ((binaries.app, "rust-runtime-app"), (binaries.observer, "runtime_observer")):
        destination = directory / name
        shutil.copy2(source, destination)
        hashes[name] = digest(destination)
    for version in (1, 2):
        destination = directory / f"package-v{version}"
        shutil.copytree(ROOT / "crates/samples/rust-runtime-app/pkg", destination)
        if version == 2:
            upgrade_template(destination)
        for name in PACKAGE_FILES:
            relative = f"package-v{version}/{name}"
            hashes[relative] = digest(directory / relative)
    metadata = {"version": 2, "dependency_graph": binaries.dependency_graph,
                "source_revision": run("git", "rev-parse", "HEAD").strip(), "sha256": hashes}
    (directory / "build-info.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")


def load_bundle(directory: Path) -> Binaries:
    directory = directory.resolve(strict=True)
    metadata = json.loads((directory / "build-info.json").read_text(encoding="utf-8"))
    if not isinstance(metadata, dict) or metadata.get("version") != 2:
        raise RuntimeError("unsupported runtime binary bundle")
    graph = metadata.get("dependency_graph")
    validate_dependency_graph(graph)
    hashes = metadata.get("sha256")
    expected_names = {"rust-runtime-app", "runtime_observer"} | {
        f"package-v{version}/{name}" for version in (1, 2) for name in PACKAGE_FILES
    }
    if not isinstance(hashes, dict) or set(hashes) != expected_names:
        raise RuntimeError("missing runtime binary hashes")
    entries = []
    for entry in directory.rglob("*"):
        entries.append(entry)
        if len(entries)>64:
            raise RuntimeError("runtime bundle entry bound")
    expected_directories = {parent.as_posix() for name in expected_names for parent in Path(name).parents if parent != Path(".")}
    if any(entry.is_symlink() for entry in entries) or {
        entry.relative_to(directory).as_posix() for entry in entries if entry.is_file()
    } != expected_names | {"build-info.json"} or {
        entry.relative_to(directory).as_posix() for entry in entries if entry.is_dir()
    } != expected_directories:
        raise RuntimeError("unexpected/symlinked runtime bundle entries")
    for name, expected in hashes.items():
        binary = directory / name
        if binary.is_symlink() or not binary.is_file() or (
            name in {"rust-runtime-app", "runtime_observer"} and not os.access(binary, os.X_OK)
        ):
            raise RuntimeError(f"missing regular executable in runtime bundle: {name}")
        if not isinstance(expected, str) or not re.fullmatch(r"[a-f0-9]{64}",expected) or digest(binary) != expected:
            raise RuntimeError(f"runtime bundle binary hash mismatch: {name}")
    return Binaries(directory / "rust-runtime-app", directory / "runtime_observer", graph,
                    directory, digest(directory / "build-info.json"))


def validate_allocation(state: dict, container: dict, name: str, owner: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,39}", name):
        raise ValueError("invalid allocation name")
    labels = container["Config"].get("Labels", {})
    if (
        state["name"] != name
        or state["owner"] != owner
        or state["project"] != f"sf-onebox-{name}"
        or state["runtime_version"] != "11.8.121.4"
        or labels.get(OWNER_LABEL) != owner
        or labels.get("com.docker.compose.project") != state["project"]
        or labels.get("com.docker.compose.service") != "onebox"
        or not container["State"]["Running"]
    ):
        raise RuntimeError("allocation ownership/version drift; refusing mutation")
    volume = f"sf-onebox-{name}_data"
    if not any(m.get("Name") == volume and m.get("Destination") == "/var/lib/sf-node" for m in container["Mounts"]):
        raise RuntimeError("owned data-volume mismatch")
    return container["Id"]


class Allocation:
    def __init__(self, name: str, owner: str):
        self.name, self.owner = name, owner
        self.container_id = self.verify()
        # Reuse the full controller guard (frozen image/config/mount identities
        # and actual node version/readiness) before our first mutation.
        run("python3", "scripts/onebox_ctl.py", "status", name, timeout=120)

    def verify(self) -> str:
        state = json.loads((ROOT / "build" / "oneboxes" / self.name / "state.json").read_text())
        containers = json.loads(run("docker", "inspect", f"sf-onebox-{self.name}-onebox-1"))
        if len(containers) != 1:
            raise RuntimeError("ambiguous execution container")
        cid = validate_allocation(state, containers[0], self.name, self.owner)
        if hasattr(self, "container_id") and cid != self.container_id:
            raise RuntimeError("container incarnation changed")
        return cid

    def execute(self, *args: str, timeout: float = 120) -> str:
        cid = self.verify()
        return run("docker", "exec", cid, *args, timeout=timeout)

    def sf(self, *args: str, timeout: float = 120) -> str:
        return self.execute("sfctl", *args, timeout=timeout)


# Runs in the container PID namespace. pidfd binds the signal to the verified
# incarnation, even if a PID is recycled between inspection and signal delivery.
PROCESS = r"""
import json,os,signal,sys,time,select,socket,ctypes,hashlib
from pathlib import Path
expected=json.loads(sys.argv[1]); action=sys.argv[2]; pid=expected["native_pid"]
def require(condition,message):
    if not condition:
        raise RuntimeError(message)
require(isinstance(pid,int) and pid>1,"invalid application PID")
fd=os.pidfd_open(pid)
try:
    root=Path("/proc")/str(pid)
    exe=os.readlink(root/"exe")
    require(exe==expected["native_entry_point"] and exe.endswith("/rust-runtime-app"),"application executable changed")
    env=dict(v.split("=",1) for v in (root/"environ").read_bytes().decode().split("\0") if "=" in v)
    rpc=expected["rpc"]
    require(env["Fabric_ApplicationHostId"]==rpc["host"],"host incarnation changed")
    require(env["Fabric_ApplicationName"]==rpc["service"].rsplit("/",1)[0],"application identity changed")
    require(int(env["Fabric_CodePackageInstanceSeqNum"])==rpc["code_sequence"],"code incarnation changed")
    stat=(root/"stat").read_text().rsplit(") ",1)[1].split()
    paths=sorted({line.split(maxsplit=5)[5] for line in (root/"maps").read_text().splitlines()
                  if len(line.split(maxsplit=5))==6 and line.split(maxsplit=5)[5].startswith("/")})
    require(not any("fabricruntime" in p.lower() or "fabriccommon" in p.lower() for p in paths),"native SF application mapping")
    result={"pid":pid,"exe":exe,"start_ticks":int(stat[19]),"parent_pid":int(stat[1]),
            "loaded_files":paths,"host":rpc["host"],"action":action,
            "activation_environment":{k:env[k] for k in (
                "Fabric_ApplicationId","Fabric_ServicePackageName","Fabric_ServicePackageVersionInstance",
                "Fabric_CodePackageInstanceSeqNum","Fabric_ServicePackageInstanceSeqNum") if k in env}}
    hasher=hashlib.sha256()
    with (root/"exe").open("rb") as binary:
        for chunk in iter(lambda:binary.read(1048576),b""):hasher.update(chunk)
    result["executable_sha256"]=hasher.hexdigest()
    if action=="kill":
        result["terminated_at_monotonic"]=time.monotonic()
        signal.pidfd_send_signal(fd,signal.SIGKILL)
    elif action=="cut-ipc":
        # Destroy only the single established TCP socket owned by this verified
        # app, selected using its actual node-injected IPC destination and fd
        # inode. No node or unrelated process is signalled.
        require(rpc.get("deliberately_noncooperative") is True,"authority test requires explicit failure fixture")
        remote_port=int(env["Fabric_RuntimeConnectionAddress"].rsplit(":",1)[1])
        descriptors={}
        for path in (root/"fd").iterdir():
            try: target=os.readlink(path)
            except FileNotFoundError: continue
            if target.startswith("socket:["): descriptors[target[8:-1]]=int(path.name)
        inodes=set(descriptors)
        matches=[]
        for line in (root/"net/tcp").read_text().splitlines()[1:]:
            row=line.split()
            local,remote=row[1].split(":"),row[2].split(":")
            if row[9] in inodes and row[3]=="01" and local[0]==remote[0]=="0100007F" and int(remote[1],16)==remote_port:
                matches.append(row[9])
        require(len(matches)==1,"cannot prove unique application IPC socket")
        # Test-only Linux x86-64 pidfd_getfd: duplicate this verified app's exact
        # IPC descriptor, recheck its inode, then shut down the shared socket.
        # No SF libraries/helper runtime, node pause, broad filter or policy
        # bypass. An unavailable kernel capability is a qualification blocker.
        require(os.uname().machine=="x86_64","fault injector requires Linux x86-64")
        libc=ctypes.CDLL(None,use_errno=True)
        duplicate=libc.syscall(438,fd,descriptors[matches[0]],0)
        require(duplicate>=0,"pidfd_getfd unavailable: errno "+str(ctypes.get_errno()))
        with socket.socket(fileno=duplicate) as ipc:
            require(os.fstat(ipc.fileno()).st_ino==int(matches[0]),"IPC descriptor was reused")
            result["authority_loss_at_monotonic"]=time.monotonic()
            ipc.shutdown(socket.SHUT_RDWR)
        poll=select.poll();poll.register(fd,select.POLLIN)
        require(bool(poll.poll(1500)),"application did not fence after exact IPC loss")
        result["authority_fence_seconds"]=time.monotonic()-result["authority_loss_at_monotonic"]
        require(result["authority_fence_seconds"]<=1,"authority fence exceeded 1 second")
    elif action!="inspect":
        raise ValueError("unsupported process action")
    print(json.dumps(result))
finally:
    os.close(fd)
"""


def observe(service: str, key: str | None = None, *, observer: Path = DEFAULT_OBSERVER,
            command: str | None = None) -> dict:
    args = [str(observer), "127.0.0.1:19000", service]
    if key is not None or command is not None:
        args.append(key if key is not None else "-")
    if command is not None:
        args.append(command)
    lines = run(*args, timeout=45 if command else 15).splitlines()
    if len(lines) != 1:
        raise RuntimeError("singleton acceptance requires exactly one endpoint")
    observed = json.loads(lines[0])
    if observed["resolver"] != "mssf-client" or observed["rpc"]["service"] != service:
        raise RuntimeError("independent observation identity mismatch")
    if observed["rpc"]["pid"] != observed["native_pid"]:
        raise RuntimeError("native query did not confirm serving process")
    return observed


def wait_observe(service: str, *, old: dict | None = None, started: float | None = None,
                 key: str | None = None, observer: Path = DEFAULT_OBSERVER) -> dict:
    started = time.monotonic() if started is None else started
    last = "no attempt"
    while time.monotonic() - started < 180:
        try:
            value = observe(service, key, observer=observer)
            if old and (
                value["native_pid"] == old["native_pid"]
                or value["rpc"]["host"] == old["rpc"]["host"]
                or value["native_entry_point_instance"] == old["native_entry_point_instance"]
            ):
                raise RuntimeError("replacement identity has not changed")
            if time.monotonic() - started > 180:
                raise RuntimeError("RPC completed outside recovery bound")
            return value
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            last = str(error)
            time.sleep(0.25)
    raise RuntimeError(f"native resolution/RPC did not recover within 180s: {last}")


def restart(allocation: Allocation, service: str, *, observer: Path = DEFAULT_OBSERVER) -> dict:
    initial = wait_observe(service, observer=observer)
    before = json.loads(allocation.execute("python3", "-c", PROCESS, json.dumps(initial), "inspect"))
    terminated = json.loads(allocation.execute("python3", "-c", PROCESS, json.dumps(initial), "kill"))
    replacement = wait_observe(service, old=initial, started=terminated["terminated_at_monotonic"],
                               observer=observer)
    elapsed = time.monotonic() - terminated["terminated_at_monotonic"]
    after = json.loads(allocation.execute("python3", "-c", PROCESS, json.dumps(replacement), "inspect"))
    return {"initial": initial, "initial_process": before, "termination": terminated,
            "replacement": replacement, "replacement_process": after, "recovery_seconds": elapsed}


def package(run_id: str, app: Path, *, templates: Path | None = None, upgrade: bool = False) -> tuple[Path, str, str]:
    source = templates / ("package-v2" if upgrade else "package-v1") if templates else ROOT / "crates/samples/rust-runtime-app/pkg"
    destination = ROOT / "build/rust-runtime" / (run_id + ("-upgrade" if upgrade else ""))
    shutil.copytree(source, destination)  # Refuse any pre-existing generated path.
    if upgrade and templates is None:
        upgrade_template(destination)
    code = destination / "RustRuntimeService/Code"
    code.mkdir()
    shutil.copy2(app, code / "rust-runtime-app")
    path = destination / "ApplicationManifest.xml"
    manifest = ET.parse(path)
    type_name = f"FubeRustRuntime{run_id}Type"
    manifest.getroot().set("ApplicationTypeName", type_name)
    manifest.write(path, encoding="utf-8", xml_declaration=True)
    return destination, type_name, manifest.getroot().attrib["ApplicationTypeVersion"]


def wait_condition(check, description: str, timeout: float = 60):
    end=time.monotonic()+timeout
    last="not attempted"
    while time.monotonic()<end:
        try:
            value=check()
            if value:
                return value
        except (RuntimeError,subprocess.TimeoutExpired) as error:
            last=str(error)
        time.sleep(0.1)
    raise RuntimeError(f"{description} not established: {last}")


def native_read(allocation: Allocation, path: str) -> dict:
    """Read-only native gateway observation, never application report delivery."""
    code = ("import json,sys,urllib.request;"
            "response=urllib.request.urlopen('http://127.0.0.1:19080'+sys.argv[1],timeout=10);"
            "print(json.dumps(json.load(response)))")
    return json.loads(allocation.execute("python3", "-c", code, path, timeout=15))


def require_same_instance(value: dict, expected: dict) -> None:
    if any(value["rpc"][field] != expected["rpc"][field]
           for field in ("partition", "instance", "incarnation", "pid", "host")):
        raise RuntimeError("report/sibling observation changed native runtime identity")


def load_value(query: dict, name: str, value: int) -> bool:
    # Stateless native FM query maps secondary retained entries to primary.
    metrics = query["PrimaryLoadMetricReports"]
    return any(metric["Name"] == name and metric["Value"] == value
               and metric["LastReportedUtc"] not in ("0001-01-01T00:00:00.000Z", "", None)
               for metric in metrics)


def health_event(query: dict, property_name: str) -> dict | None:
    matches = [event for event in query["HealthEvents"]
               if event["SourceId"] == "RustRuntime"
               and event["Property"] == property_name]
    if len(matches) > 1:
        raise RuntimeError("duplicate native health event identity")
    return matches[0] if matches else None


def reporting_acceptance(allocation: Allocation, alpha: dict, beta: dict, result: dict,
                         *, observer: Path = DEFAULT_OBSERVER) -> dict:
    service = alpha["rpc"]["service"]
    partition = str(uuid.UUID(alpha["rpc"]["partition"]))
    instance = alpha["rpc"]["instance"]
    load_path = f"/Partitions/{partition}/$/GetLoadInformation?api-version=6.0"
    health_path = f"/Partitions/{partition}/$/GetReplicas/{instance}/$/GetHealth?api-version=6.0"

    def command(query, *, expect_success=True):
        observed = observe(service, "int:0", observer=observer, command=query)
        require_same_instance(observed, alpha)
        report = observed["rpc"]["report"]
        if report["ok"] is not expect_success:
            raise RuntimeError("unexpected runtime report result: " + json.dumps(report))
        return observed

    def load(name, value):
        query = native_read(allocation, load_path)
        return query if load_value(query, name, value) else None

    def event(property_name, sequence, state, *, expired=False):
        query = native_read(allocation, health_path)
        result["hm_last_raw"] = query
        if (query["PartitionId"] != partition or int(query["InstanceId"]) != instance
                or query["ServiceKind"] != "Stateless"):
            raise RuntimeError("native HM health identity mismatch")
        item = health_event(query, property_name)
        if item and (int(item["SequenceNumber"]) == sequence
                     and item["HealthState"] == state
                     and item["IsExpired"] is expired):
            return query
        return None

    result["load_write"] = command("op=load&value=731")
    result["fm_load"] = wait_condition(lambda: load("RuntimeWork", 731), "native FM report load", 90)
    result["move_write"] = command("op=move")
    result["fm_move_cost"] = wait_condition(lambda: load("_MoveCost_", 3), "native FM move-cost retention", 90)
    result["move_evidence_scope"] = "FM retained _MoveCost_, not RAP echo or proof placement configuration uses it"
    result["health_write"] = command("op=health&property=Ready&state=2&sequence=100&ttl=0&remove=false")
    result["hm_warning"] = wait_condition(lambda: event("Ready", 100, "Warning"), "native HM Warning sequence100", 90)
    result["stale_health_result"] = command("op=health&property=Ready&state=3&sequence=99&ttl=0&remove=false", expect_success=False)
    if result["stale_health_result"]["rpc"]["report"]["native_code"] is None:
        raise RuntimeError("stale health report did not return an actual HM error")
    result["hm_stale_unchanged"] = event("Ready", 100, "Warning")
    if result["hm_stale_unchanged"] is None:
        raise RuntimeError("older health sequence changed native event")
    result["newer_health_write"] = command("op=health&property=Ready&state=1&sequence=101&ttl=0&remove=false")
    result["hm_newer_ok"] = wait_condition(lambda: event("Ready", 101, "Ok"), "native HM newer health", 90)

    result["expiring_write"] = command("op=health&property=Retain&state=1&sequence=200&ttl=3&remove=false")
    result["hm_before_expiry"] = wait_condition(lambda: event("Retain", 200, "Ok"), "native unexpired event", 20)
    result["hm_expired_retained"] = wait_condition(lambda: event("Retain", 200, "Ok", expired=True), "native expired retained event", 90)
    if result["hm_expired_retained"]["AggregatedHealthState"] != "Error":
        raise RuntimeError("native retained expired report did not evaluate Error")
    result["clear_expired_write"] = command("op=health&property=Retain&state=1&sequence=201&ttl=0&remove=false")
    result["hm_expired_renewed"] = wait_condition(lambda: event("Retain", 201, "Ok"), "native higher sequence renews expiry", 90)
    result["removable_write"] = command("op=health&property=Remove&state=1&sequence=300&ttl=3&remove=true")
    result["hm_before_remove"] = wait_condition(lambda: event("Remove", 300, "Ok"), "native removable initial event", 20)
    def removed():
        query = native_read(allocation, health_path)
        return query if health_event(query, "Remove") is None else None
    result["hm_removed_from_query"] = wait_condition(removed, "native remove-on-expiration", 90)

    fault_started = time.monotonic()
    result["runtime_transient_fault"] = command("op=fault")
    def replacement():
        value = observe(service, "int:0", observer=observer)
        if (value["rpc"]["instance"] == instance
                or value["rpc"]["incarnation"] == alpha["rpc"]["incarnation"]):
            return None
        if value["rpc"]["partition"] != partition or value["native_pid"] != beta["native_pid"]:
            raise RuntimeError("fault replacement changed partition or killed shared-process sibling")
        return value
    renewed = wait_condition(replacement, "runtime transient native instance replacement and resolved RPC", 180)
    result["replacement"] = renewed
    result["fault_recovery_seconds"] = time.monotonic() - fault_started
    sibling = observe(service, "int:1", observer=observer)
    require_same_instance(sibling, beta)
    result["sibling_after_fault"] = sibling
    result["closed_reports"] = observe(service, "int:0", observer=observer,
        command=f"op=stale&instance={instance}")
    require_same_instance(result["closed_reports"], renewed)
    if result["closed_reports"]["rpc"]["report"]["ok"] is not True:
        raise RuntimeError("closed/stale partition report probes failed")
    # Do not mistake retained pre-replacement FM data for a new-instance report.
    result["replacement_load_write"] = observe(service, "int:0", observer=observer, command="op=load&value=947")
    require_same_instance(result["replacement_load_write"], renewed)
    if result["replacement_load_write"]["rpc"]["report"]["ok"] is not True:
        raise RuntimeError("replacement partition cannot report load")
    result["fm_replacement_load"] = wait_condition(lambda: load("RuntimeWork", 947), "new active instance load", 90)
    result["reporting_process"] = json.loads(allocation.execute("python3", "-c", PROCESS, json.dumps(renewed), "inspect"))
    return renewed


def controls_and_authority(allocation: Allocation, application: str, result: dict | None = None,
                           *, observer: Path = DEFAULT_OBSERVER, expected_generation: int | None = None) -> dict:
    from concurrent.futures import ThreadPoolExecutor
    import http.client
    from urllib.parse import urlsplit
    import threading

    anchor=wait_observe(application+"/Singleton",observer=observer)
    constraint="NodeType == "+anchor["rpc"]["node_type"]
    result={} if result is None else result
    result["anchor"]=anchor
    def create(suffix,partitioned=False):
        args=["service","create","--app-id",application.removeprefix("fabric:/"),
              "--name",application+"/"+suffix,"--service-type","RustRuntimeType",
              "--stateless","--instance-count","1","--activation-mode","SharedProcess",
              "--constraints",constraint]
        args+=["--int-scheme","--int-scheme-count","2","--int-scheme-low","0","--int-scheme-high","1"] if partitioned else ["--singleton-scheme"]
        allocation.sf(*args)
    def metric(name,minimum,key=None,service="Concurrent",abort_min=0):
        v=observe(application+"/"+service,key,observer=observer)
        if v["native_pid"]!=anchor["native_pid"]:
            raise RuntimeError("unrelated partition moved/crashed during control qualification")
        if service=="Concurrent" and key=="int:1" and (
            v["rpc"]["instance"]!=beta["rpc"]["instance"] or v["rpc"]["incarnation"]!=beta["rpc"]["incarnation"]
        ):
            raise RuntimeError("sibling instance/incarnation changed during target-only operation")
        counters=v["rpc"]["native_actions"]
        return v if counters[name]>=minimum and counters["abort_callbacks"]>=abort_min else None
    create("Concurrent",True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        alpha,beta=list(pool.map(lambda key:wait_observe(application+"/Concurrent",key=key,observer=observer),["int:0","int:1"]))
    if not (alpha["native_pid"]==beta["native_pid"]==anchor["native_pid"]
            and alpha["rpc"]["partition"]!=beta["rpc"]["partition"]
            and alpha["rpc"]["maximum_concurrent_opens"]>=2):
        raise RuntimeError("native shared-process concurrent partitions/overlapping Opens not proven")
    result["concurrent"]={"alpha":alpha,"beta":beta}
    if expected_generation is not None and any(
        v["rpc"]["creation_generation"] != expected_generation or v["rpc"]["creation_configuration"] != "after-upgrade"
        for v in (alpha,beta)
    ):
        raise RuntimeError("later native service creation received stale package context")
    result["reports"] = {}
    alpha = reporting_acceptance(allocation, alpha, beta, result["reports"], observer=observer)
    baseline=alpha["rpc"]["native_actions"]
    query=json.loads(allocation.sf("replica","deployed","--node-name",alpha["rpc"]["node"],
        "--partition-id",alpha["rpc"]["partition"],"--replica-id",str(alpha["rpc"]["instance"])))
    if query.get("serviceKind")!="Stateless" or query.get("serviceName")!=application+"/Concurrent":
        raise RuntimeError("native correlated stateless query identity mismatch")
    result["native_query"]=query
    result["query_observation"]=wait_condition(lambda:metric("query",baseline["query"]+1,"int:0"),"native RAPQuery")
    allocation.sf("service","update","--service-id",(application+"/Concurrent").removeprefix("fabric:/").replace("/","~"),
                  "--stateless","--move-cost","High")
    result["update_observation"]=wait_condition(lambda:metric("update",baseline["update"]+2,"int:0"),"native description updates")
    # Non-force permanent removal is native Abort=4; force-remove instead kills
    # the host and is explicitly not accepted as Abort callback evidence.
    allocation.sf("replica","remove","--node-name",alpha["rpc"]["node"],
                  "--partition-id",alpha["rpc"]["partition"],"--replica-id",str(alpha["rpc"]["instance"]))
    result["abort_observation"]=wait_condition(lambda:metric("abort",baseline["abort"]+1,"int:1",
        abort_min=baseline["abort_callbacks"]+1),"native Abort command and completed cleanup")
    renewed=wait_condition(lambda: (v if (v:=observe(application+"/Concurrent","int:0",observer=observer))["rpc"]["instance"]!=alpha["rpc"]["instance"] else None),
                           "native replacement of removed partition")
    if renewed["native_pid"]!=beta["native_pid"]:
        raise RuntimeError("partition Abort disturbed shared-process sibling")
    result["partition_replacement"]=renewed
    result["abort_closed_reports"] = observe(application+"/Concurrent", "int:0", observer=observer,
        command=f"op=stale&instance={alpha['rpc']['instance']}")
    require_same_instance(result["abort_closed_reports"], renewed)
    if result["abort_closed_reports"]["rpc"]["report"]["ok"] is not True:
        raise RuntimeError("aborted partition reports were not rejected")
    failures_before=renewed["rpc"]["native_actions"]
    create("FailClose")
    wait_observe(application+"/FailClose",observer=observer)
    allocation.sf("service","delete","--service-id",(application+"/FailClose").removeprefix("fabric:/").replace("/","~"))
    result["close_failure_observation"]=wait_condition(
        lambda:metric("close_failures",failures_before["close_failures"]+1,"int:1",
            abort_min=failures_before["abort_callbacks"]+1),"native close failure/abort cleanup")
    create("FailOpen")
    result["open_failure_observation"]=wait_condition(
        lambda:metric("open_failures",failures_before["open_failures"]+1,"int:1",
            abort_min=failures_before["abort_callbacks"]+2),"native open failure and completed cleanup")
    try:
        observe(application+"/FailOpen",observer=observer)
    except RuntimeError:
        result["failed_open_not_published"]=True
    else:
        raise RuntimeError("failed open published a usable endpoint")
    allocation.sf("service","delete","--service-id",(application+"/FailOpen").removeprefix("fabric:/").replace("/","~"))
    allocation.sf("service","delete","--service-id",(application+"/Concurrent").removeprefix("fabric:/").replace("/","~"))
    create("Uncooperative")
    noncooperative=wait_observe(application+"/Uncooperative",observer=observer)
    if noncooperative["native_pid"]!=anchor["native_pid"]:
        raise RuntimeError("authority fixture did not share the proven application process")
    result["noncooperative"]=noncooperative
    endpoint=urlsplit(noncooperative["endpoint"])
    stop=threading.Event();ready=threading.Event();observations=[]
    def probe():
        while not stop.is_set():
            connection=http.client.HTTPConnection(endpoint.hostname,endpoint.port,timeout=.15)
            try:
                connection.request("GET",endpoint.path)
                response=connection.getresponse()
                payload=json.loads(response.read(16384))
                if response.status==200 and payload.get("pid")==noncooperative["native_pid"]:
                    observations.append((time.monotonic(),payload.get("remaining_authority_millis")))
                    ready.set()
            except (OSError,ValueError,http.client.HTTPException):
                pass
            finally:
                connection.close()
            stop.wait(.01)
    thread=threading.Thread(target=probe)
    thread.start()
    try:
        if not ready.wait(3):
            raise RuntimeError("noncooperative application listener was not usable before fault")
        fence=json.loads(allocation.execute("python3","-c",PROCESS,json.dumps(noncooperative),"cut-ipc",timeout=6))
    finally:
        stop.set();thread.join(timeout=1)
    triggered=fence["authority_loss_at_monotonic"]
    if any(at>triggered+1 for at,_ in observations):
        raise RuntimeError("old application RPC survived authority hard deadline")
    if not any(at>=triggered and remaining==0 for at,remaining in observations):
        raise RuntimeError("noncooperative serving after invalidation was not actually exercised")
    result["authority_fence"]=fence
    result["old_process_rpc_samples"]=len(observations)
    result["last_usable_old_rpc_after_fault_seconds"]=max(at for at,_ in observations)-triggered
    return result


def items(raw: str) -> list:
    value=json.loads(raw)
    return value if isinstance(value,list) else value["items"]


def deployment_artifacts(allocation: Allocation, observation: dict) -> dict:
    config = observation["rpc"]["packages"]["configuration"]
    script = r"""
import json,sys
from pathlib import Path
config=Path(sys.argv[1]); base=config.parent
paths=sorted(set(base.glob("App.*.xml"))|set(base.glob("*.Package.Current.xml"))|set(base.glob("*.Manifest.*.xml"))|{config/"Settings.xml"})
if len(paths)>16: raise RuntimeError("artifact evidence count limit")
result={}
for p in paths:
    if p.stat().st_size>1048576: raise RuntimeError("artifact evidence byte limit")
    result[str(p)]=p.read_text()
print(json.dumps(result))
"""
    files = json.loads(allocation.execute("python3", "-c", script, str(Path(config["path"]))))
    current = [ET.fromstring(value) for path,value in files.items() if path.endswith(".Package.Current.xml")]
    if len(current) != 1:
        raise RuntimeError("ambiguous native current package evidence")
    root = current[0]
    code = root.find(f"{{{NS}}}DigestedCodePackage")
    native_config = root.find(f"{{{NS}}}DigestedConfigPackage")
    settings = ET.fromstring(files[str(Path(config["path"]) / "Settings.xml")])
    value = settings.find(f"{{{NS}}}Section[@Name='Main']/{{{NS}}}Parameter[@Name='Value']").attrib["Value"]
    override = native_config.find(f"{{{NS}}}ConfigOverride/{{{NS}}}Settings/{{{NS}}}Section/{{{NS}}}Parameter[@Name='Override']").attrib["Value"]
    if (value,override) != (config["value"],config["override"]):
        raise RuntimeError("RPC configuration differs from independent native settings/override artifacts")
    if native_config.attrib["RolloutVersion"] != config["rollout"]:
        raise RuntimeError("RPC config rollout differs from native artifact")
    if root.attrib["ManifestVersion"] != config["manifest_version"]:
        raise RuntimeError("RPC manifest differs from native artifact")
    return {"files":files,"code_rollout":code.attrib["RolloutVersion"],
            "code_checksum":code.attrib.get("ContentChecksum",""),
            "config_rollout":native_config.attrib["RolloutVersion"],
            "service_rollout":root.attrib["RolloutVersion"],"settings_value":value,"override":override}


def configuration_upgrade(allocation: Allocation, binaries: Binaries, result: dict, attempts: dict) -> dict:
    service = result["application"] + "/Singleton"
    initial = wait_observe(service, observer=binaries.observer)
    if initial["rpc"]["packages"]["configuration"]["value"] != "before-upgrade":
        raise RuntimeError("missing pre-upgrade configuration")
    evidence = {"initial":initial,
                "initial_process":json.loads(allocation.execute("python3","-c",PROCESS,json.dumps(initial),"inspect")),
                "initial_artifacts":deployment_artifacts(allocation,initial),
                "samples":[],"sample_errors":[]}
    result["configuration_upgrade"] = evidence  # Preserve partial evidence on failure.
    source,type_name,version = package(result["run_id"], binaries.app,templates=binaries.template_root,upgrade=True)
    if type_name != result["type_name"] or version == result["version"]:
        raise RuntimeError("upgrade package identity mismatch")
    remote = f"/var/lib/sf-node/fube-rust-runtime-{result['run_id']}-upgrade"
    result.setdefault("upload_paths",[]).append(remote)
    run("docker","cp",str(source),f"{allocation.verify()}:{remote}")
    allocation.sf("application","upload","--path",remote)
    attempts.setdefault("extra_versions",{})[version] = False
    allocation.sf("application","provision","--application-type-build-path",remote.rsplit("/",1)[1])
    attempts["extra_versions"][version] = True
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        upgrade = executor.submit(allocation.sf,"application","upgrade","--application-id",
                                  result["application"].removeprefix("fabric:/"),
                                  "--application-version",version,"--parameters","[]","--mode","UnmonitoredAuto")
        end = time.monotonic()+180
        while True:
            try:
                observation = observe(service,observer=binaries.observer)
                require_same_instance(observation,initial)
                if observation["native_entry_point_instance"] != initial["native_entry_point_instance"]:
                    raise RuntimeError("native entrypoint instance restarted during unchanged-code upgrade")
                if observation["rpc"]["packages"]["runtime_id"] != initial["rpc"]["packages"]["runtime_id"]:
                    raise RuntimeError("runtime identity changed during config-only upgrade")
                evidence["samples"].append({"at":time.monotonic(),"observation":observation})
            except Exception as error:
                evidence["sample_errors"].append(str(error))
                raise
            if upgrade.done():
                upgrade.result()
                status = json.loads(allocation.sf("application","upgrade-status","--application-id",
                                     result["application"].removeprefix("fabric:/"),"--timeout","10",timeout=15))
                evidence["native_upgrade_status"] = status
                if status.get("upgradeState") == "RollingForwardCompleted":
                    break
                if status.get("upgradeState") in {"RollingBackCompleted","Failed"}:
                    raise RuntimeError("native upgrade did not roll forward")
            if time.monotonic() >= end or len(evidence["samples"]) >= 256:
                raise RuntimeError("configuration-only upgrade deadline/sample bound")
            time.sleep(0.25)
    final = observe(service,observer=binaries.observer)
    evidence["final"] = final
    require_same_instance(final,initial)
    p = final["rpc"]["packages"]
    before = initial["rpc"]["packages"]
    if p["configuration"]["value"] != "after-upgrade" or p["generation"] <= before["generation"]:
        raise RuntimeError("native upgrade completed without current configuration publication")
    if p["initial_configuration"]["value"] != "before-upgrade" or p["retained_configuration"]["value"] != "before-upgrade":
        raise RuntimeError("retained old context/package settings changed")
    if p["initial_configuration"]["path"] != before["configuration"]["path"]:
        raise RuntimeError("retained old package path changed")
    if p["unregistered_callbacks"] != 0 or [e["registration"] for e in p["events"]] != [1,2]:
        raise RuntimeError("package subscription registration/unregistration/order mismatch")
    if any(e["previous"]["value"] != "before-upgrade" or e["current"]["value"] != "after-upgrade"
           or e["published_generation"] != p["generation"] for e in p["events"]):
        raise RuntimeError("package event ownership/publication mismatch")
    evidence["final_process"] = json.loads(allocation.execute("python3","-c",PROCESS,json.dumps(final),"inspect"))
    if evidence["initial_process"]["start_ticks"] != evidence["final_process"]["start_ticks"]:
        raise RuntimeError("native process start time changed during config-only upgrade")
    if any(v["executable_sha256"] != digest(binaries.app) for v in (evidence["initial_process"],evidence["final_process"])):
        raise RuntimeError("deployed Code bytes do not match prebuilt bundle")
    evidence["final_artifacts"] = deployment_artifacts(allocation,final)
    for field in ("code_rollout","code_checksum"):
        if evidence["initial_artifacts"][field] != evidence["final_artifacts"][field]:
            raise RuntimeError("config-only fixture changed native code identity")
    if initial["native_code_version"] != final["native_code_version"]:
        raise RuntimeError("config-only fixture changed native code version")
    evidence["status"] = "PASS_UNCHANGED_CODE_NATIVE_CONTEXT_REFRESH"
    evidence["directory_lifetime"] = "Observed existence only; retained handles do not pin native directories."
    return evidence


def cleanup_deployment(allocation: Allocation, result: dict, attempts: dict) -> None:
    application, type_name, version=result["application"],result["type_name"],result["version"]
    cleanup=result["cleanup"]
    try:
        if attempts["create"]:
            apps=items(allocation.sf("application","list",timeout=10))
            app=next((v for v in apps if v["name"]==application),None)
            if app:
                if app["typeName"]!=type_name or app["typeVersion"] not in {version,*attempts.get("extra_versions",{})}:
                    raise RuntimeError("minted application identity drift; refusing deletion")
                allocation.sf("application","delete","--application-id",application.removeprefix("fabric:/"))
                deadline=time.monotonic()+60
                while any(v["name"]==application for v in items(allocation.sf("application","list",timeout=10))):
                    if time.monotonic()>=deadline:
                        raise RuntimeError("owned application deletion did not complete; no force-remove")
                    time.sleep(0.25)
                cleanup["application"]="deleted; absence confirmed"
            elif not attempts["create_completed"]:
                cleanup["application"]="unconfirmed: ambiguous create and no current observation; late completion possible"
                result["errors"].append(cleanup["application"])
            else:
                cleanup["application"]="absence confirmed"
        versions = {version:attempts["provision_completed"]} if attempts["provision"] else {}
        versions.update(attempts.get("extra_versions",{}))
        for version,completed in versions.items():
            types=items(allocation.sf("application","type-list",timeout=10))
            present=any(v["name"]==type_name and v["version"]==version for v in types)
            if present:
                allocation.sf("application","unprovision","--application-type-name",type_name,
                              "--application-type-version",version)
                wait_condition(lambda: not any(v["name"]==type_name and v["version"]==version
                               for v in items(allocation.sf("application","type-list",timeout=10))),
                               "owned type version absence",timeout=60)
                cleanup.setdefault("types",{})[version]="unprovisioned; absence confirmed"
                cleanup["type"]="unprovisioned; absence confirmed"
            elif not completed:
                cleanup["type"]="unconfirmed: ambiguous provision and no current observation; late completion possible"
                result["errors"].append(cleanup["type"])
            else:
                cleanup["type"]="absence confirmed"
                cleanup.setdefault("types",{})[version]="absence confirmed"
    except Exception as error:
        cleanup["error"]=str(error)
        result["errors"].append("cleanup: "+str(error))
        cleanup["retained_for_recovery"]={"application":application,"type":type_name,"version":version}
    finally:
        for remote in result.get("upload_paths",[]):
            expected = f"/var/lib/sf-node/fube-rust-runtime-{result['run_id']}"
            if remote not in {expected,expected+"-upgrade"}:
                result["errors"].append("upload staging ownership mismatch")
                continue
            try:
                allocation.execute("python3","-c",
                    "import shutil,sys; from pathlib import Path; p=Path(sys.argv[1]); shutil.rmtree(p) if p.exists() else None; assert not p.exists()",remote)
            except Exception as error:
                result["errors"].append("upload staging cleanup: "+str(error))


def destroy_owned(name: str, owner: str) -> dict:
    """Use the existing lifecycle lock/guards, independent of cluster health."""
    with onebox_ctl.ctl.locked(onebox_ctl.STATE_ROOT,name) as directory:
        cluster=onebox_ctl.Onebox(directory,name)
        cluster.load()
        if cluster.state["owner"]!=owner:
            raise RuntimeError("successful-create owner changed; cleanup refused")
        return cluster.destroy(name)


def create_owned(name: str) -> str:
    """Capture this creation's owner while its existing controller lock is held."""
    onebox_ctl.ctl.check_local_docker()
    previous_umask=os.umask(0o077)
    try:
        with onebox_ctl.ctl.locked(onebox_ctl.STATE_ROOT,name) as directory:
            cluster=onebox_ctl.Onebox(directory,name)
            cluster.create(onebox_ctl.DEFAULT_SUBNET,"sf-apps-onebox:"+onebox_ctl.ctl.RUNTIME,900)
            # Return the original in-memory creation capability, never whichever
            # owner appears in a later name-based state-file read.
            owner=cluster.state["owner"]
            if not isinstance(owner,str) or not re.fullmatch(r"[a-f0-9]{32}",owner):
                raise RuntimeError("creation did not provide a valid owner capability")
            return owner
    finally:
        os.umask(previous_umask)


def qualify(allocation: Allocation, full: bool = False, binaries: Binaries | None = None) -> dict:
    run_id = uuid.uuid4().hex[:12]
    application = f"fabric:/FubeRustRuntime{run_id}"
    type_name=f"FubeRustRuntime{run_id}Type"
    result = {"run_id": run_id, "onebox": allocation.name, "owner": allocation.owner,
              "container_id": allocation.container_id, "sf_version": "11.8.121.4",
              "application": application, "type_name": type_name, "version": "1.1",
              "status":"FAIL","errors":[],"cleanup":{}}
    attempts={"provision":False,"provision_completed":False,"create":False,"create_completed":False}
    try:
        binaries = build_binaries() if binaries is None else binaries
        needed = run("readelf", "-d", str(binaries.app))
        if any(name in needed.lower() for name in ("fabricruntime", "fabriccommon")):
            raise RuntimeError("application links native SF libraries")
        result.update(dependency_graph=binaries.dependency_graph,elf_dynamic=needed,
                      binary_sha256={"rust-runtime-app":digest(binaries.app),
                                     "runtime_observer":digest(binaries.observer)})
        result["bundle_manifest_sha256"] = binaries.bundle_manifest_sha256
        source, type_name, version = package(run_id, binaries.app,templates=binaries.template_root)
        result.update(type_name=type_name,version=version)
        remote=f"/var/lib/sf-node/fube-rust-runtime-{run_id}"
        allocation.sf("cluster", "select", "--endpoint", "http://127.0.0.1:19080")
        if any(v["name"]==application for v in items(allocation.sf("application","list",timeout=10))):
            raise RuntimeError("minted application collision; no cleanup capability granted")
        if any(v["name"]==type_name for v in items(allocation.sf("application","type-list",timeout=10))):
            raise RuntimeError("minted type collision; no cleanup capability granted")
        result["upload_paths"]=[remote]
        run("docker", "cp", str(source), f"{allocation.verify()}:{remote}")
        allocation.sf("application", "upload", "--path", remote)
        attempts["provision"]=True
        allocation.sf("application", "provision", "--application-type-build-path", remote.rsplit("/", 1)[1])
        attempts["provision_completed"]=True
        attempts["create"]=True
        allocation.sf("application", "create", "--app-name", application, "--app-type", type_name, "--app-version", version)
        attempts["create_completed"]=True
        configuration_upgrade(allocation,binaries,result,attempts)
        if full:
            result["resilience"]={}
            generation=result["configuration_upgrade"]["final"]["rpc"]["packages"]["generation"]
            controls_and_authority(allocation,application,result["resilience"],observer=binaries.observer,
                                   expected_generation=generation)
        result["restart"] = restart(allocation, application + "/Singleton",observer=binaries.observer)
    except Exception as error:
        result["errors"].append("qualification: "+str(error))
    finally:
        cleanup_deployment(allocation,result,attempts)
    if not result["errors"]:
        result["status"]="PASS_FULL_STATELESS_PROFILE" if full else "PASS_SINGLETON_PLACEMENT_RPC_NATIVE_RESTART"
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", nargs="?")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--build-bundle", type=Path, help="build binaries/evidence into a new directory without a onebox")
    mode.add_argument("--prebuilt", type=Path, help="use a verified binary bundle; never build or invoke Cargo")
    parser.add_argument("--existing-owner", help="exact owner capability; never destroy caller-owned onebox")
    parser.add_argument("--restart-service", help="qualify replacement of an explicitly named existing owned service")
    parser.add_argument("--full",action="store_true",help="native shared-process/control/callback/authority qualification")
    parser.add_argument("--report",type=Path,help="write a new structured evidence file (never overwrite)")
    args = parser.parse_args()
    if sys.platform!="linux":
        parser.error("native application-runtime qualification requires Linux x86-64")
    if args.build_bundle is not None:
        if args.name or args.existing_owner or args.restart_service or args.full or args.report:
            parser.error("--build-bundle cannot be combined with qualification options")
        build_bundle(args.build_bundle)
        print(json.dumps({"bundle": str(args.build_bundle), "status": "BUILT"}))
        return
    if args.name is None:
        parser.error("an allocation name is required for qualification")
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,39}", args.name):
        parser.error("invalid allocation name")
    if args.restart_service and not args.existing_owner:
        parser.error("--restart-service requires --existing-owner")
    created = False
    owner = None
    result={"status":"FAIL","onebox":args.name,"errors":[],"cleanup":{}}
    try:
        binaries = load_bundle(args.prebuilt) if args.prebuilt is not None else None
        if args.existing_owner:
            owner = args.existing_owner
        else:
            # Creation and capability capture are one controller transaction.
            owner=create_owned(args.name)
            created = True
        allocation = Allocation(args.name, owner)
        if args.restart_service:
            observer = binaries.observer if binaries is not None else DEFAULT_OBSERVER
            result = restart(allocation, args.restart_service, observer=observer)
        else:
            result = qualify(allocation,args.full,binaries)
        result.setdefault("status","PASS_SINGLETON_PLACEMENT_RPC_NATIVE_RESTART")
        result.setdefault("errors",[])
        result.setdefault("cleanup",{})
    except Exception as error:
        result["errors"].append("execution: "+str(error))
    finally:
        if created:
            try:
                if owner is None:
                    raise RuntimeError("successful-create owner unavailable; preserve resources for recovery")
                result["cleanup"]["onebox"]=destroy_owned(args.name,owner)
            except Exception as error:
                result["errors"].append("onebox cleanup: "+str(error))
                result["cleanup"]["onebox"]="unconfirmed; owned state retained for recovery"
        else:
            result["cleanup"]["onebox"]="caller-owned allocation retained" if args.existing_owner else "create rejected; no cleanup capability"
    if result["errors"]:
        result["status"]="FAIL"
    if args.report:
        try:
            with args.report.open("x",encoding="utf-8") as file:
                json.dump(result,file,indent=2)
        except Exception as error:
            result["errors"].append("evidence file: "+str(error))
            result["status"]="FAIL"
    print(json.dumps(result,indent=2))
    if result["status"]=="FAIL":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
