#!/usr/bin/env python3
"""Native application-runtime qualification, not the standalone Rust test host.

Default: create/qualify/destroy one exact owned onebox. --existing-owner is an
explicit capability for a caller-owned allocation; it never destroys that box.
Only normal management deployment is performed through sfctl. All application
hosting/lifecycle is Rust IPC; resolution is independently mssf-client.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import time
import uuid
import xml.etree.ElementTree as ET
import onebox_ctl

ROOT = Path(__file__).resolve().parents[1]
OWNER_LABEL = "io.github.service-fabric-apps.cluster-owner"
NS = "http://schemas.microsoft.com/2011/01/fabric"
ET.register_namespace("", NS)


def run(*args: str, timeout: float = 120) -> str:
    result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"{args[0]} failed ({result.returncode}): {result.stderr[-6000:]}")
    return result.stdout


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
import json,os,signal,sys,time
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
            "loaded_files":paths,"host":rpc["host"],"action":action}
    if action=="kill":
        result["terminated_at_monotonic"]=time.monotonic()
        signal.pidfd_send_signal(fd,signal.SIGKILL)
    elif action!="inspect":
        raise ValueError("unsupported process action")
    print(json.dumps(result))
finally:
    os.close(fd)
"""


def observe(service: str, key: str | None = None) -> dict:
    args = [str(ROOT / "target/debug/examples/runtime_observer"), "127.0.0.1:19000", service]
    if key is not None:
        args.append(key)
    lines = run(*args, timeout=15).splitlines()
    if len(lines) != 1:
        raise RuntimeError("singleton acceptance requires exactly one endpoint")
    observed = json.loads(lines[0])
    if observed["resolver"] != "mssf-client" or observed["rpc"]["service"] != service:
        raise RuntimeError("independent observation identity mismatch")
    if observed["rpc"]["pid"] != observed["native_pid"]:
        raise RuntimeError("native query did not confirm serving process")
    return observed


def wait_observe(service: str, *, old: dict | None = None, started: float | None = None, key: str | None = None) -> dict:
    started = time.monotonic() if started is None else started
    last = "no attempt"
    while time.monotonic() - started < 180:
        try:
            value = observe(service, key)
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


def restart(allocation: Allocation, service: str) -> dict:
    initial = wait_observe(service)
    before = json.loads(allocation.execute("python3", "-c", PROCESS, json.dumps(initial), "inspect"))
    terminated = json.loads(allocation.execute("python3", "-c", PROCESS, json.dumps(initial), "kill"))
    replacement = wait_observe(service, old=initial, started=terminated["terminated_at_monotonic"])
    elapsed = time.monotonic() - terminated["terminated_at_monotonic"]
    after = json.loads(allocation.execute("python3", "-c", PROCESS, json.dumps(replacement), "inspect"))
    return {"initial": initial, "initial_process": before, "termination": terminated,
            "replacement": replacement, "replacement_process": after, "recovery_seconds": elapsed}


def package(run_id: str) -> tuple[Path, str, str]:
    source = ROOT / "crates/samples/rust-runtime-app/pkg"
    destination = ROOT / "build/rust-runtime" / run_id
    shutil.copytree(source, destination)  # Refuse any pre-existing generated path.
    code = destination / "RustRuntimeService/Code"
    code.mkdir()
    shutil.copy2(ROOT / "target/debug/rust-runtime-app", code / "rust-runtime-app")
    path = destination / "ApplicationManifest.xml"
    manifest = ET.parse(path)
    type_name = f"FubeRustRuntime{run_id}Type"
    manifest.getroot().set("ApplicationTypeName", type_name)
    manifest.write(path, encoding="utf-8", xml_declaration=True)
    return destination, type_name, manifest.getroot().attrib["ApplicationTypeVersion"]


def items(raw: str) -> list:
    value=json.loads(raw)
    return value if isinstance(value,list) else value["items"]


def cleanup_deployment(allocation: Allocation, result: dict, attempts: dict) -> None:
    application, type_name, version=result["application"],result["type_name"],result["version"]
    cleanup=result["cleanup"]
    try:
        if attempts["create"]:
            apps=items(allocation.sf("application","list",timeout=10))
            app=next((v for v in apps if v["name"]==application),None)
            if app:
                if app["typeName"]!=type_name or app["typeVersion"]!=version:
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
        if attempts["provision"]:
            types=items(allocation.sf("application","type-list",timeout=10))
            present=any(v["name"]==type_name and v["version"]==version for v in types)
            if present:
                allocation.sf("application","unprovision","--application-type-name",type_name,
                              "--application-type-version",version)
                cleanup["type"]="unprovisioned"
            elif not attempts["provision_completed"]:
                cleanup["type"]="unconfirmed: ambiguous provision and no current observation; late completion possible"
                result["errors"].append(cleanup["type"])
            else:
                cleanup["type"]="absence confirmed"
    except Exception as error:
        cleanup["error"]=str(error)
        result["errors"].append("cleanup: "+str(error))
        cleanup["retained_for_recovery"]={"application":application,"type":type_name,"version":version}


def destroy_owned(name: str, owner: str) -> dict:
    """Use the existing lifecycle lock/guards, independent of cluster health."""
    with onebox_ctl.ctl.locked(onebox_ctl.STATE_ROOT,name) as directory:
        cluster=onebox_ctl.Onebox(directory,name)
        cluster.load()
        if cluster.state["owner"]!=owner:
            raise RuntimeError("successful-create owner changed; cleanup refused")
        return cluster.destroy(name)


def qualify(allocation: Allocation) -> dict:
    run_id = uuid.uuid4().hex[:12]
    application = f"fabric:/FubeRustRuntime{run_id}"
    type_name=f"FubeRustRuntime{run_id}Type"
    result = {"run_id": run_id, "onebox": allocation.name, "owner": allocation.owner,
              "container_id": allocation.container_id, "sf_version": "11.8.121.4",
              "application": application, "type_name": type_name, "version": "1.1",
              "status":"FAIL","errors":[],"cleanup":{}}
    attempts={"provision":False,"provision_completed":False,"create":False,"create_completed":False}
    try:
        run("cargo", "build", "-p", "rust-runtime-app")
        run("cargo", "build", "-p", "mssf-client", "--example", "runtime_observer")
        graph = run("cargo", "tree", "-p", "rust-runtime-app")
        if any(name in graph.lower() for name in ("mssf-core", "mssf-com", "fabricruntime", "fabriccommon")):
            raise RuntimeError("application dependency graph contains native SF adapters")
        needed = run("readelf", "-d", "target/debug/rust-runtime-app")
        if any(name in needed.lower() for name in ("fabricruntime", "fabriccommon")):
            raise RuntimeError("application links native SF libraries")
        result.update(dependency_graph=graph,elf_dynamic=needed)
        source, type_name, version = package(run_id)
        result.update(type_name=type_name,version=version)
        remote=f"/tmp/fube-rust-runtime-{run_id}"
        allocation.sf("cluster", "select", "--endpoint", "http://127.0.0.1:19080")
        if any(v["name"]==application for v in items(allocation.sf("application","list",timeout=10))):
            raise RuntimeError("minted application collision; no cleanup capability granted")
        if any(v["name"]==type_name for v in items(allocation.sf("application","type-list",timeout=10))):
            raise RuntimeError("minted type collision; no cleanup capability granted")
        run("docker", "cp", str(source), f"{allocation.verify()}:{remote}")
        allocation.sf("application", "upload", "--path", remote)
        attempts["provision"]=True
        allocation.sf("application", "provision", "--application-type-build-path", remote.rsplit("/", 1)[1])
        attempts["provision_completed"]=True
        attempts["create"]=True
        allocation.sf("application", "create", "--app-name", application, "--app-type", type_name, "--app-version", version)
        attempts["create_completed"]=True
        result["restart"] = restart(allocation, application + "/Singleton")
    except Exception as error:
        result["errors"].append("qualification: "+str(error))
    finally:
        cleanup_deployment(allocation,result,attempts)
    if not result["errors"]:
        result["status"]="PASS_SINGLETON_PLACEMENT_RPC_NATIVE_RESTART"
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name")
    parser.add_argument("--existing-owner", help="exact owner capability; never destroy caller-owned onebox")
    parser.add_argument("--restart-service", help="qualify replacement of an explicitly named existing owned service")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,39}", args.name):
        parser.error("invalid allocation name")
    if args.restart_service and not args.existing_owner:
        parser.error("--restart-service requires --existing-owner")
    created = False
    owner = None
    result={"status":"FAIL","onebox":args.name,"errors":[],"cleanup":{}}
    try:
        if args.existing_owner:
            owner = args.existing_owner
        else:
            # The repository controller is the only allocation implementation.
            run("python3", "scripts/onebox_ctl.py", "create", args.name, timeout=1000)
            created = True
            owner = json.loads((ROOT / "build/oneboxes" / args.name / "state.json").read_text())["owner"]
        allocation = Allocation(args.name, owner)
        result = restart(allocation, args.restart_service) if args.restart_service else qualify(allocation)
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
    print(json.dumps(result,indent=2))
    if result["status"]=="FAIL":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
