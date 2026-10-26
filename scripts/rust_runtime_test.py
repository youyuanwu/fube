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
import json,os,signal,sys,time,select,socket,ctypes
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


def controls_and_authority(allocation: Allocation, application: str, result: dict | None = None) -> dict:
    from concurrent.futures import ThreadPoolExecutor
    import http.client
    from urllib.parse import urlsplit
    import threading

    anchor=wait_observe(application+"/Singleton")
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
        v=observe(application+"/"+service,key)
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
        alpha,beta=list(pool.map(lambda key:wait_observe(application+"/Concurrent",key=key),["int:0","int:1"]))
    if not (alpha["native_pid"]==beta["native_pid"]==anchor["native_pid"]
            and alpha["rpc"]["partition"]!=beta["rpc"]["partition"]
            and alpha["rpc"]["maximum_concurrent_opens"]>=2):
        raise RuntimeError("native shared-process concurrent partitions/overlapping Opens not proven")
    result["concurrent"]={"alpha":alpha,"beta":beta}
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
    renewed=wait_condition(lambda: (v if (v:=observe(application+"/Concurrent","int:0"))["rpc"]["instance"]!=alpha["rpc"]["instance"] else None),
                           "native replacement of removed partition")
    if renewed["native_pid"]!=beta["native_pid"]:
        raise RuntimeError("partition Abort disturbed shared-process sibling")
    result["partition_replacement"]=renewed
    failures_before=renewed["rpc"]["native_actions"]
    create("FailClose")
    wait_observe(application+"/FailClose")
    allocation.sf("service","delete","--service-id",(application+"/FailClose").removeprefix("fabric:/").replace("/","~"))
    result["close_failure_observation"]=wait_condition(
        lambda:metric("close_failures",failures_before["close_failures"]+1,"int:1",
            abort_min=failures_before["abort_callbacks"]+1),"native close failure/abort cleanup")
    create("FailOpen")
    result["open_failure_observation"]=wait_condition(
        lambda:metric("open_failures",failures_before["open_failures"]+1,"int:1",
            abort_min=failures_before["abort_callbacks"]+2),"native open failure and completed cleanup")
    try:
        observe(application+"/FailOpen")
    except RuntimeError:
        result["failed_open_not_published"]=True
    else:
        raise RuntimeError("failed open published a usable endpoint")
    allocation.sf("service","delete","--service-id",(application+"/FailOpen").removeprefix("fabric:/").replace("/","~"))
    allocation.sf("service","delete","--service-id",(application+"/Concurrent").removeprefix("fabric:/").replace("/","~"))
    create("Uncooperative")
    noncooperative=wait_observe(application+"/Uncooperative")
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


def qualify(allocation: Allocation, full: bool = False) -> dict:
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
        if full:
            result["resilience"]={}
            controls_and_authority(allocation,application,result["resilience"])
    except Exception as error:
        result["errors"].append("qualification: "+str(error))
    finally:
        cleanup_deployment(allocation,result,attempts)
    if not result["errors"]:
        result["status"]="PASS_FULL_STATELESS_PROFILE" if full else "PASS_SINGLETON_PLACEMENT_RPC_NATIVE_RESTART"
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name")
    parser.add_argument("--existing-owner", help="exact owner capability; never destroy caller-owned onebox")
    parser.add_argument("--restart-service", help="qualify replacement of an explicitly named existing owned service")
    parser.add_argument("--full",action="store_true",help="native shared-process/control/callback/authority qualification")
    parser.add_argument("--report",type=Path,help="write a new structured evidence file (never overwrite)")
    args = parser.parse_args()
    if sys.platform!="linux":
        parser.error("native application-runtime qualification requires Linux x86-64")
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
        result = restart(allocation, args.restart_service) if args.restart_service else qualify(allocation,args.full)
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
