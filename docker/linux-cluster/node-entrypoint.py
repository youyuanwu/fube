import fcntl
import grp
import ipaddress
import json
import os
from pathlib import Path
import pwd
import signal
import socket
import subprocess
import sys
import time
import xml.etree.ElementTree as ET


NAMESPACE = {"sf": "http://schemas.microsoft.com/2011/01/fabric"}
ROOT = Path("/var/lib/sf-node")
BIN = Path("/opt/microsoft/servicefabric/bin")
CODE = BIN / "Fabric" / "Fabric.Code"
MANIFEST = Path("/run/sf-bootstrap/ClusterManifest.xml")
IDENTITY_FIELDS = (
    "cluster_id",
    "node_name",
    "node_ip",
    "image_id",
    "runtime_version",
    "volume_name",
)
ONEBOX_NODES = ("N0010", "N0020", "N0030", "N0040", "N0050")
HOST_SHUTDOWN_TIMEOUT = 90
AUXILIARY_SHUTDOWN_TIMEOUT = 30


def expected_identity(environ):
    identity = {name: environ["SF_" + name.upper()] for name in IDENTITY_FIELDS}
    if "SF_LAYOUT" in environ:
        if environ["SF_LAYOUT"] != "onebox":
            raise ValueError("Unsupported deployment layout")
        identity["layout"] = "onebox"
    if any(not value for value in identity.values()):
        raise ValueError("Node identity fields must not be empty")
    if not identity["node_name"].isascii() or not identity["node_name"].isalnum():
        raise ValueError("Node name must contain only ASCII letters and digits")
    address = ipaddress.IPv4Address(identity["node_ip"])
    if address.is_loopback or address.is_unspecified or address.is_multicast:
        raise ValueError("Node IP must be a routable unicast address")
    return identity


def validate_manifest(path, identity):
    manifest = ET.parse(path).getroot()
    if manifest.get("Name") != identity["cluster_id"]:
        raise ValueError("Manifest cluster identity does not match the node")
    infrastructure = manifest.find("sf:Infrastructure/sf:Linux", NAMESPACE)
    onebox = identity.get("layout") == "onebox"
    scale_min = infrastructure.get("IsScaleMin", "false") if infrastructure is not None else None
    if scale_min not in (("true", "1") if onebox else ("false", "0")):
        raise ValueError(f"Expected Linux IsScaleMin={'true' if onebox else 'false'}")
    nodes = infrastructure.findall("sf:NodeList/sf:Node", NAMESPACE)
    matches = [node for node in nodes if node.get("IPAddressOrFQDN") == identity["node_ip"]]
    wanted = set(ONEBOX_NODES) if onebox else {identity["node_name"]}
    if len(matches) != len(wanted) or {node.get("NodeName") for node in matches} != wanted:
        raise ValueError("Manifest must match exactly the assigned local node")
    names = [node.get("NodeName") for node in nodes]
    addresses = [node.get("IPAddressOrFQDN") for node in nodes]
    if len(set(names)) != len(names) or (not onebox and len(set(addresses)) != len(addresses)):
        raise ValueError("Duplicate manifest node name or address")
    if onebox:
        if len(nodes) != len(wanted) or len({node.get("NodeTypeRef") for node in nodes}) != len(wanted):
            raise ValueError("Onebox requires five local nodes with distinct node types")
        ports = [
            endpoint.get("Port")
            for endpoint in manifest.findall("sf:NodeTypes/sf:NodeType/sf:Endpoints/*", NAMESPACE)
            if endpoint.get("Port") is not None
        ]
        if len(ports) != len(set(ports)):
            raise ValueError("Onebox node endpoints must use distinct ports")
    return manifest


def read_identity(root, expected):
    path = root / "identity.json"
    if not path.is_file():
        raise ValueError("Missing volume identity; allocate a fresh node volume explicitly")
    record = json.loads(path.read_text())
    if not isinstance(record, dict) or any(
        record.get(name) != value for name, value in expected.items()
    ):
        raise ValueError("Volume identity does not match the assigned node/image")
    if record.get("phase") not in ("allocated", "configured"):
        raise ValueError("Interrupted bootstrap; preserve state and recover explicitly")
    if record["phase"] == "allocated":
        unexpected = set(root.iterdir()) - {path, root / ".entrypoint.lock"}
        if unexpected:
            raise ValueError("Fresh bootstrap refused: allocated volume contains data")
    return record


def save_identity(root, record):
    temporary = root / "identity.json.tmp"
    with temporary.open("w") as output:
        json.dump(record, output, indent=2)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(root / "identity.json")
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def run(arguments, **kwargs):
    subprocess.run([str(argument) for argument in arguments], check=True, timeout=120, **kwargs)


def prepare_platform(root):
    accounts = {}
    for name in ("sfuser", "sfappsuser"):
        account = pwd.getpwnam(name)
        accounts[name] = [account.pw_uid, account.pw_gid]
        home = Path(account.pw_dir)
        home.mkdir(parents=True, exist_ok=True)
        os.chown(home, account.pw_uid, account.pw_gid)
    for name in ("ServiceFabricAllowedUsers", "ServiceFabricAdministrators"):
        try:
            grp.getgrnam(name)
        except KeyError:
            run(["groupadd", name])
    for name in ("sfuser", "sfappsuser", "root"):
        run(["usermod", "-a", "-G", "ServiceFabricAllowedUsers", name])
    for directory in (root, root / "Data", root / "log"):
        directory.mkdir(exist_ok=True)
        os.chown(directory, *accounts["sfuser"])
    for directory in ("/var/lib/waagent", "/var/lib/sfcerts", "/tmp/.service.fabric"):
        Path(directory).mkdir(parents=True, exist_ok=True)
    run(["setfacl", "-m", "d:g:ServiceFabricAllowedUsers:rwx,g:ServiceFabricAllowedUsers:rwx",
         "/tmp/.service.fabric"])
    settings = {
        "FabricDataRoot": root / "Data",
        "FabricLogRoot": root / "log",
        "FabricBinRoot": BIN,
        "FabricCodePath": CODE,
    }
    Path("/etc/servicefabric").mkdir(exist_ok=True)
    for name, value in settings.items():
        (Path("/etc/servicefabric") / name).write_text(str(value))
        os.environ[name] = str(value)
    os.environ["LD_LIBRARY_PATH"] = str(CODE) + ":" + os.environ.get("LD_LIBRARY_PATH", "")
    return accounts


def validate_deployment(root, identity):
    packages = set((root / "Data").glob("*/Fabric/Fabric.Package.current.xml"))
    names = ONEBOX_NODES if identity.get("layout") == "onebox" else (identity["node_name"],)
    expected = {root / "Data" / name / "Fabric" / "Fabric.Package.current.xml" for name in names}
    if packages != expected:
        raise ValueError("Deployment must contain exactly the assigned local node")
    for package in expected:
        validate_manifest(package.with_name("ClusterManifest.current.xml"), identity)


def own_fresh_deployment(data, account):
    os.chown(data, *account, follow_symlinks=False)
    for parent, directories, files in os.walk(data):
        for name in directories + files:
            os.chown(Path(parent) / name, *account, follow_symlinks=False)


def prepare_deployment(root, identity, record):
    if (root / "Data" / "TargetInformation.xml").exists():
        raise ValueError("Pending TargetInformation.xml requires explicit recovery")
    if record["phase"] == "allocated":
        validate_manifest(MANIFEST, identity)
        record["phase"] = "configuring"
        save_identity(root, record)
        run([CODE / "FabricDeployer.sh", "/operation:ValidateClusterManifest",
             "/cm:" + str(MANIFEST)])
        run([CODE / "FabricDeployer.sh", "/operation:Create",
             "/fabricBinRoot:" + str(BIN), "/fabricDataRoot:" + str(root / "Data"),
             "/cm:" + str(MANIFEST)])
        own_fresh_deployment(root / "Data", record["accounts"]["sfuser"])
    else:
        validate_deployment(root, identity)
    run([CODE / "FabricDeployer.sh"], cwd=CODE)
    validate_deployment(root, identity)
    record["phase"] = "configured"
    save_identity(root, record)


def stop_process(process, timeout=AUXILIARY_SHUTDOWN_TIMEOUT, *,
                 name="process", stop_signal=signal.SIGTERM):
    if process.poll() is None:
        process.send_signal(stop_signal)
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            print(f"ERROR: {name} (pid {process.pid}) exceeded {timeout}s shutdown deadline; "
                  "sending SIGKILL",
                  file=sys.stderr, flush=True)
            process.kill()
            process.wait()
            return False
    return True


def supervise(command, auxiliaries):
    requested = []
    previous = {}

    def request_stop(number, _frame):
        if not requested:
            requested.append(number)

    for number in (signal.SIGTERM, signal.SIGINT):
        previous[number] = signal.signal(number, request_stop)
    try:
        host = subprocess.Popen(command)
        while host.poll() is None and not requested:
            failed = [process for process in auxiliaries if process.poll() is not None]
            if failed:
                print(f"ERROR: required auxiliary process {failed[0].pid} exited",
                      file=sys.stderr, flush=True)
                stop_process(host, HOST_SHUTDOWN_TIMEOUT, name="FabricHost")
                return 1
            time.sleep(0.1)
        if requested:
            if not stop_process(host, HOST_SHUTDOWN_TIMEOUT, name="FabricHost",
                                stop_signal=requested[0]):
                return 1
            return 0
        print(f"ERROR: FabricHost exited unexpectedly: {host.returncode}",
              file=sys.stderr, flush=True)
        return 1
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def start_tracing(root):
    daemon = subprocess.Popen(["lttng-sessiond", "--no-kernel"])
    try:
        for _ in range(100):
            if daemon.poll() is not None:
                raise RuntimeError("LTTng session daemon exited during startup")
            probe = subprocess.run(["lttng", "list"], capture_output=True, timeout=5)
            if probe.returncode == 0:
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("LTTng session daemon did not become ready")
        run(["lttng", "create", "ServiceFabric_0", "--output=" + str(root / "log" / "traces")])
        run(["lttng", "enable-channel", "--userspace", "--subbuf-size=8192",
             "--num-subbuf=2", "--tracefile-size=8388608", "--tracefile-count=2", "channel0"])
        run(["lttng", "enable-event", "--userspace", "--channel=channel0", "service_fabric:*"])
        run(["lttng", "add-context", "--userspace", "--type=vtid", "--type=vpid"])
        run(["lttng", "start"])
        return daemon
    except (OSError, RuntimeError, subprocess.SubprocessError):
        stop_process(daemon, name="LTTng session daemon")
        raise


def main():
    if os.geteuid() != 0:
        raise ValueError("Node initialization requires root inside the container")
    identity = expected_identity(os.environ)
    version = subprocess.check_output(
        ["dpkg-query", "-W", "-f=${Version}", "servicefabric"], text=True
    ).strip()
    if version != identity["runtime_version"]:
        raise ValueError("Installed SF version differs from the assigned runtime")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((identity["node_ip"], 0))
    with (ROOT / ".entrypoint.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        record = read_identity(ROOT, identity)
        accounts = prepare_platform(ROOT)
        if "accounts" in record and record["accounts"] != accounts:
            raise ValueError("SF account ownership differs from the retained deployment")
        record["accounts"] = accounts
        tracing = start_tracing(ROOT)
        try:
            prepare_deployment(ROOT, identity, record)
            print(f"Starting node {identity['node_name']}; SSH is disabled", flush=True)
            return supervise([str(BIN / "FabricHost"), "-c", "-skipfabricsetup"], [tracing])
        finally:
            if not stop_process(tracing, name="LTTng session daemon"):
                raise RuntimeError("LTTng session daemon required forced termination")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyError, OSError, ValueError, RuntimeError, ET.ParseError,
            subprocess.SubprocessError) as error:
        print(f"ERROR: node initialization/lifecycle failed: {error}", file=sys.stderr, flush=True)
        sys.exit(1)
