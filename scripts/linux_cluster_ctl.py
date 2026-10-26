"""Named, single-host development clusters; Docker owns resources, SF owns health."""

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

from linux_cluster import SHUTDOWN_TIMEOUT, check_subnet, generate


REPO = Path(__file__).resolve().parents[1]
STATE_ROOT = REPO / "build/linux-clusters"
DEFAULT_SUBNET = "172.30.90.0/24"
OWNER_LABEL = "io.github.service-fabric-apps.cluster-owner"
IMAGE_LABEL = "io.github.service-fabric-apps.linux-cluster"
RUNTIME = "11.8.121.4"
PACKAGE_SHA = "fbcb0b8d52e84b7e2bf9c336d9e0ece8cb9ff7fa1de53367d56c6ff8ab850c83"
FABRIC_SHA = "c1bdc6523fea1a3711b2a4cd4d7cfa57c1133254a23c521c8054ad4d1ff0249f"
PACKAGE = f"/opt/sf-install/servicefabric_{RUNTIME}.deb"
NODES = ("n0", "n1", "n2")
PHASES = ("allocating", "starting", "baseline", "ready", "down", "destroying", "destroyed")
SEED_VOLUME = """
import json, os, pathlib, sys
p = pathlib.Path('/var/lib/sf-node')
wanted = json.load(sys.stdin)
identity = p / 'identity.json'
if identity.exists():
    if json.loads(identity.read_text()) != wanted or set(p.iterdir()) != {identity}:
        raise SystemExit('Volume is not an unchanged allocation; refusing identity reuse')
else:
    if any(p.iterdir()):
        raise SystemExit('Volume is not empty; refusing identity reuse')
    with identity.open('x') as f:
        json.dump(wanted, f)
        f.flush()
        os.fsync(f.fileno())
    fd = os.open(p, os.O_RDONLY | os.O_DIRECTORY)
    os.fsync(fd)
    os.close(fd)
"""
READ_VOLUME = """
import json, pathlib, sys
p = pathlib.Path('/var/lib/sf-node')
wanted = json.load(sys.stdin)
record = json.loads((p / 'identity.json').read_text())
if any(record.get(k) != v for k, v in wanted.items() if k != 'phase'):
    raise SystemExit('Volume identity differs; refusing reuse')
if record.get('phase') not in ('allocated', 'configured'):
    raise SystemExit('Interrupted node bootstrap; explicit recovery required')
if record['phase'] == 'allocated' and set(p.iterdir()) - {p/'identity.json', p/'.entrypoint.lock'}:
    raise SystemExit('Allocated volume contains unexpected data')
if (p / 'Data/TargetInformation.xml').exists():
    raise SystemExit('Pending native deployment; explicit recovery required')
print(json.dumps({'node': wanted['node_name'], 'phase': record['phase']}))
"""


def run(arguments, *, timeout=30, data=None):
    result = subprocess.run(
        [str(argument) for argument in arguments], input=data, text=True,
        capture_output=True, timeout=timeout, check=True,
    )
    if result.stderr.strip():
        print(result.stderr.rstrip(), file=sys.stderr)
    return result.stdout


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, value):
    temporary = path.with_name(path.name + ".tmp")
    with open(temporary, "w", opener=lambda name, flags: os.open(name, flags, 0o600)) as output:
        output.write(value)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_json(path, value):
    write(path, json.dumps(value, indent=2) + "\n")


def validate_name(name, prefix="sf-linux-"):
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,39}", name):
        raise ValueError("Cluster name must be 1-40 lowercase letters/digits/hyphens, starting with a letter")
    return prefix + name


def check_local_docker():
    context = os.environ.get("DOCKER_CONTEXT")
    host = os.environ.get("DOCKER_HOST")
    if context or not host:
        arguments = ["docker", "context", "inspect"]
        if context:
            arguments.append(context)
        host = json.loads(run(arguments))[0]["Endpoints"]["docker"]["Host"]
    if not host.startswith("unix://"):
        raise ValueError("Use a local Unix-socket Docker context; remote Docker hosts are unsupported")
    platform = run(["docker", "info", "--format", "{{.OSType}}/{{.Architecture}}"]).strip()
    if platform not in ("linux/x86_64", "linux/amd64"):
        raise ValueError("The cluster requires a Linux x86-64 Docker daemon")


@contextmanager
def locked(root, name):
    validate_name(name)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory = root / name
    if directory.is_symlink():
        raise ValueError("Cluster state directory must not be a symlink")
    with (root / f".{name}.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("Another lifecycle operation holds this cluster's lock") from error
        yield directory


def definition(state, directory, generator=generate, gateway_service="n0"):
    # Use the recorded XML; never regenerate file-store secrets during recovery.
    compose, inventory, _ = generator(
        state["project"], state["subnet"], state["image"], state["image"],
        directory / "ClusterManifest.xml",
    )
    label = {OWNER_LABEL: state["owner"]}
    for name, volume in compose["volumes"].items():
        volume.update(external=True, name=f"{state['project']}_{name}")
    compose["networks"]["default"]["labels"] = label
    for service in compose["services"].values():
        service["labels"] = label
        service["pull_policy"] = "never"
        if "stop_grace_period" in service:
            # Older allocations keep the grace period of their frozen image/inputs.
            service["stop_grace_period"] = f"{state.get('shutdown_timeout', 45)}s"
    # Older allocations retain their frozen, unpublished gateway configuration.
    if state.get("publish_gateway", False):
        compose["services"][gateway_service]["ports"] = ["127.0.0.1:19080:19080"]
    if "tools" in compose["services"]:
        tools = compose["services"]["tools"]
        tools["profiles"] = ["admin"]
        tools["environment"] = {"HOME": "/tmp/sfctl-home"}
        tools["volumes"] = [{
            "type": "bind", "source": str(directory / "ClusterManifest.xml"),
            "target": "/run/sf-bootstrap/ClusterManifest.xml", "read_only": True,
        }]
    return compose, inventory["nodes"]


class Cluster:
    project_prefix = "sf-linux-"
    image_repository = "sf-apps-node"
    image_label = IMAGE_LABEL
    services = NODES
    admin_service = "tools"
    requires_baseline = True
    generate = staticmethod(generate)
    definition = staticmethod(definition)

    def __init__(self, directory, name):
        self.directory = directory
        self.name = name
        self.project = validate_name(name, self.project_prefix)
        self.path = directory / "state.json"
        self.state = None
        self.deadline = None

    def save(self, **changes):
        self.state.update(changes)
        write_json(self.path, self.state)

    def event(self, event, **values):
        with open(
            self.directory / "events.jsonl", "a",
            opener=lambda name, flags: os.open(name, flags, 0o600),
        ) as output:
            output.write(json.dumps({
                "time": time.time(), "owner": self.state["owner"], "event": event, **values,
            }) + "\n")
            output.flush()
            os.fsync(output.fileno())

    def load(self):
        if not self.path.is_file():
            raise ValueError(f"Unknown cluster {self.name}; use create, not implicit allocation")
        self.state = json.loads(self.path.read_text())
        state = self.state
        if not isinstance(state, dict) or state.get("schema") != 1 or state.get("name") != self.name \
                or state.get("project") != self.project \
                or state.get("phase") not in PHASES \
                or not isinstance(state.get("publish_gateway", False), bool) \
                or state.get("shutdown_timeout", 45) not in (45, SHUTDOWN_TIMEOUT) \
                or not re.fullmatch(r"[0-9a-f]{32}", state.get("owner", "")):
            raise ValueError("Invalid cluster identity/state")
        if state["phase"] == "destroyed":
            return
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", state.get("image", "")) \
                or state.get("runtime_version") != RUNTIME or state.get("manifest_version") != "1":
            raise ValueError("Unsupported image/runtime/manifest identity")
        if state.get("baseline") not in ("not-started", "submitting", "accepted", "complete"):
            raise ValueError("Invalid native baseline journal")
        expected, nodes = self.definition(state, self.directory)
        if state.get("nodes") != nodes:
            raise ValueError("Recorded node allocation differs from the fixed seed topology")
        if digest(self.directory / "ClusterManifest.xml") != state["manifest_sha256"] \
                or json.loads((self.directory / "compose.json").read_text()) != expected:
            raise ValueError("Generated inputs changed; refusing lifecycle mutation")

    def compose(self, *arguments, timeout=SHUTDOWN_TIMEOUT + 30):
        return run(
            ["docker", "compose", "--project-name", self.project, "--file",
             self.directory / "compose.json", "--profile", "admin", *arguments],
            timeout=self.remaining(timeout),
        )

    def remaining(self, limit):
        if self.deadline is None:
            return limit
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Lifecycle deadline exceeded; state preserved")
        return min(limit, remaining)

    @property
    def image_tag(self):
        return self.image_repository + ":cluster-" + self.state["owner"]

    def image_tag_id(self):
        image = run([
            "docker", "image", "ls", "--no-trunc", "--filter", "reference=" + self.image_tag,
            "--format", "{{.ID}}",
        ]).strip()
        if image and image != self.state["image"]:
            raise ValueError("Retained image tag no longer matches the frozen image identity")
        return image

    def retain_image(self):
        if not self.image_tag_id():
            run(["docker", "image", "tag", self.state["image"], self.image_tag])
            self.event("image-retained", tag=self.image_tag)

    def resources(self, require_volumes=True):
        ids = run(["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={self.project}"]).split()
        containers = json.loads(run(["docker", "inspect", *ids])) if ids else []
        volume_names = set(run(["docker", "volume", "ls", "--format", "{{.Name}}"]).split())
        wanted = {node["volume_name"] for node in self.state["nodes"]}
        missing = wanted - volume_names
        if missing and require_volumes:
            raise ValueError("Missing retained volume(s); refusing replacement: " + ", ".join(sorted(missing)))
        volumes = json.loads(run(["docker", "volume", "inspect", *sorted(wanted & volume_names)])) \
            if wanted & volume_names else []
        network_ids = run(["docker", "network", "ls", "-q", "--filter",
                           f"name=^{self.project}_default$"]).split()
        networks = json.loads(run(["docker", "network", "inspect", *network_ids])) if network_ids else []
        for resource in [*containers, *volumes, *networks]:
            labels = resource.get("Config", {}).get("Labels") if "Config" in resource else resource.get("Labels")
            if (labels or {}).get(OWNER_LABEL) != self.state["owner"]:
                raise ValueError(f"Refusing foreign resource: {resource.get('Name', resource.get('Id'))}")
        seen = set()
        for container in containers:
            service = container["Config"]["Labels"].get("com.docker.compose.service")
            if service not in (*self.services, self.admin_service) or service in seen \
                    or container["Image"] != self.state["image"]:
                raise ValueError("Unexpected/duplicate container or image in owned project")
            seen.add(service)
            if service in self.services:
                node = next(node for node in self.state["nodes"] if node["node_name"] == service)
                mounts = [mount for mount in container["Mounts"] if mount["Destination"] == "/var/lib/sf-node"]
                if len(mounts) != 1 or mounts[0].get("Name") != node["volume_name"]:
                    raise ValueError(f"Unexpected data mount on {service}")
        return containers, volumes, networks

    def subnet_check(self):
        routes = json.loads(run(["ip", "-j", "-4", "route"]))
        ids = run(["docker", "network", "ls", "-q"]).split()
        networks = json.loads(run(["docker", "network", "inspect", *ids])) if ids else []
        own = [network for network in networks if network["Name"] == f"{self.project}_default"]
        owned_subnets = {
            config["Subnet"] for network in own
            for config in network.get("IPAM", {}).get("Config") or [] if config.get("Subnet")
        }
        if own and owned_subnets != {self.state["subnet"]}:
            raise ValueError("Owned bridge has a different subnet")
        occupied = [route["dst"] for route in routes
                    if route.get("dst", "default") != "default" and route["dst"] not in owned_subnets]
        occupied += [
            config["Subnet"] for network in networks if network not in own
            for config in network.get("IPAM", {}).get("Config") or [] if config.get("Subnet")
        ]
        check_subnet(self.state["subnet"], occupied)

    def helper(self, node, code, readonly=False):
        mount = f"type=volume,src={node['volume_name']},dst=/var/lib/sf-node"
        if readonly:
            mount += ",readonly"
        return run(
            ["docker", "run", "--rm", "--pull", "never", "-i", "--network", "none", "--entrypoint", "python3",
             "--mount", mount, self.state["image"], "-c", code],
            data=json.dumps(node), timeout=self.remaining(30),
        )

    def allocate(self):
        _, volumes, _ = self.resources(require_volumes=False)
        existing = {volume["Name"] for volume in volumes}
        for node in self.state["nodes"]:
            if node["volume_name"] not in existing:
                run(["docker", "volume", "create", "--label",
                     f"{OWNER_LABEL}={self.state['owner']}", node["volume_name"]])
                volume = json.loads(run(["docker", "volume", "inspect", node["volume_name"]]))[0]
                if (volume.get("Labels") or {}).get(OWNER_LABEL) != self.state["owner"]:
                    raise ValueError("Volume allocation collided with a foreign owner")
            self.helper(node, SEED_VOLUME)
        self.save(phase="starting")

    def create(self, subnet, image, timeout):
        if self.path.exists():
            self.load()
            if self.state["phase"] != "destroyed":
                raise ValueError("Cluster already exists; use up to resume it")
        elif self.directory.exists():
            raise ValueError("State directory exists without an allocation record; preserve and inspect it")
        image_data = json.loads(run(["docker", "image", "inspect", image]))[0]
        if image_data.get("Os") != "linux" or image_data.get("Architecture") != "amd64" \
                or (image_data["Config"].get("Labels") or {}).get(self.image_label) != "1":
            raise ValueError("Build the repository's Linux amd64 cluster image first")
        image_id = image_data["Id"]
        run(["docker", "run", "--rm", "--pull", "never", "--network", "none", "--entrypoint", "bash", image_id,
             "-ec", f"test \"$(dpkg-query -W -f='${{Version}}' servicefabric)\" = {RUNTIME}; "
             f"echo '{PACKAGE_SHA}  {PACKAGE}' | sha256sum --check -; "
             f"echo '{FABRIC_SHA}  /opt/microsoft/servicefabric/bin/Fabric/Fabric.Code/Fabric' "
             "| sha256sum --check -; test -f /usr/local/lib/sf-node/cluster-probe.py"],
            timeout=60)
        self.state = {
            "schema": 1, "name": self.name, "project": self.project,
            "owner": uuid.uuid4().hex, "image": image_id, "subnet": str(check_subnet(subnet, [])),
            "runtime_version": RUNTIME, "manifest_version": "1", "phase": "allocating",
            "baseline": "not-started", "publish_gateway": True,
            "shutdown_timeout": SHUTDOWN_TIMEOUT,
        }
        _, nodes = self.definition(self.state, self.directory)
        self.state["nodes"] = nodes
        # Reject name collisions even after a previous successful destroy.
        ids = run(["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={self.project}"]).strip()
        names = set(run(["docker", "volume", "ls", "--format", "{{.Name}}"]).split())
        networks = run(["docker", "network", "ls", "-q", "--filter",
                        f"name=^{self.project}_default$"]).strip()
        if ids or networks or names.intersection(node["volume_name"] for node in nodes):
            raise ValueError("Project/container/network/volume name already exists; refusing adoption")
        self.subnet_check()
        self.directory.mkdir(mode=0o700, exist_ok=True)
        _, _, manifest = self.generate(
            self.project, self.state["subnet"], image_id, image_id,
            self.directory / "ClusterManifest.xml",
        )
        write(self.directory / "ClusterManifest.xml", manifest)
        compose, _ = self.definition(self.state, self.directory)
        write_json(self.directory / "compose.json", compose)
        self.save(manifest_sha256=digest(self.directory / "ClusterManifest.xml"))
        self.event("allocated")
        return self.up(timeout)

    @contextmanager
    def tools(self):
        if self.admin_service in self.services:
            yield
            return
        try:
            self.compose("up", "-d", "--no-deps", "--no-build", "--pull", "never", "tools")
            yield
        finally:
            # Cleanup must still run after the readiness deadline expires.
            deadline, self.deadline = self.deadline, None
            try:
                self.compose("rm", "--stop", "--force", "tools")
            finally:
                self.deadline = deadline

    def exec(self, *arguments, timeout=60, data=None):
        return run(
            ["docker", "compose", "--project-name", self.project, "--file",
             self.directory / "compose.json", "exec", "-T", self.admin_service, *arguments],
            timeout=self.remaining(timeout), data=data,
        )

    def sfctl(self, *arguments, timeout=180):
        return self.exec("sfctl", *arguments, timeout=timeout)

    def probe(self, baseline_required=None):
        if baseline_required is None:
            baseline_required = self.requires_baseline
        budget = self.remaining(45)
        observation = json.loads(self.exec(
            "python3", "/usr/local/lib/sf-node/cluster-probe.py",
            timeout=budget + 5, data=json.dumps({
                "spec": self.state, "baseline_required": baseline_required, "budget": budget - 1,
            }),
        ))
        write_json(self.directory / "last-status.json", observation)
        return observation

    def wait(self, baseline_required, stable=30):
        healthy_since = None
        while True:
            observation = self.probe(baseline_required)
            for gateway in observation["gateways"].values():
                if gateway.get("upgrade", {}).get("UpgradeState") in (
                    "RollingBackInProgress", "RollingBackCompleted", "Failed"
                ):
                    raise ValueError("Native upgrade failed or rolled back; inspect last-status.json")
            now = time.monotonic()
            if observation["ready"]:
                if healthy_since is None:
                    healthy_since = now
                if now - healthy_since >= stable:
                    return observation
            else:
                healthy_since = None
            self.event("readiness", ready=observation["ready"], problems=observation["problems"])
            time.sleep(self.remaining(3))

    def establish_baseline(self):
        observation = self.probe(False)
        gateways = observation["gateways"]
        if any("error" in gateway for gateway in gateways.values()):
            raise ValueError("Cannot determine native baseline; inspect last-status.json")
        states = {gateway["baseline"] for gateway in gateways.values()}
        if states == {"complete"}:
            self.save(baseline="complete")
            return
        if not states.issubset({"uninitialized", "active", "complete"}):
            raise ValueError("Conflicting native baseline; refusing mutation")
        if "active" in states:
            self.save(phase="baseline", baseline="accepted")
            return
        if states != {"uninitialized"}:
            raise ValueError("Gateways disagree on native baseline; retry after convergence")
        if self.state["baseline"] in ("submitting", "accepted", "complete"):
            raise ValueError(
                "Baseline submission is ambiguous or lost; inspect native progress. "
                "Refusing to submit it again or reset data"
            )
        self.save(phase="baseline")
        endpoint = "http://" + self.state["nodes"][0]["node_ip"] + ":19080"
        self.sfctl("cluster", "select", "--endpoint", endpoint)
        upload_name = "baseline-" + self.state["owner"]
        upload = "/tmp/" + upload_name
        self.exec("mkdir", "-p", upload)
        self.exec("cp", PACKAGE, upload + "/")
        self.exec("cp", "/run/sf-bootstrap/ClusterManifest.xml", upload + "/ClusterManifest.xml")
        code = json.loads(self.sfctl("cluster", "code-versions"))
        config = json.loads(self.sfctl("cluster", "config-versions"))
        if any(not isinstance(versions, list) or any(not isinstance(item, dict) for item in versions)
               for versions in (code, config)):
            raise ValueError("Invalid provisioned-version response; refusing baseline mutation")
        provision = []
        if not any(item.get("codeVersion") == RUNTIME for item in code):
            provision += ["--code-file-path", upload_name + f"/servicefabric_{RUNTIME}.deb"]
        if not any(item.get("configVersion") == "1" for item in config):
            provision += ["--cluster-manifest-file-path", upload_name + "/ClusterManifest.xml"]
        if provision:
            self.sfctl("application", "upload", "--path", upload, "--timeout", "300", timeout=320)
            self.sfctl("cluster", "provision", *provision, "--timeout", "180", timeout=200)
        self.save(baseline="submitting")
        self.event("baseline-submit")
        self.sfctl(
            "cluster", "upgrade", "--code-version", RUNTIME, "--config-version", "1",
            "--rolling-upgrade-mode", "Monitored", "--failure-action", "Rollback",
            "--health-check-wait", "PT2S", "--health-check-stable", "PT5S",
            "--health-check-retry", "PT30S", "--upgrade-domain-timeout", "PT180S",
            "--upgrade-timeout", "PT600S", "--replica-set-check-timeout", "120",
            "--unhealthy-nodes", "0", "--unhealthy-applications", "0", "--warning-as-error",
        )
        self.save(baseline="accepted")

    def up(self, timeout, restart_nodes=()):
        if self.state["phase"] in ("destroying", "destroyed"):
            raise ValueError("Cluster is being/has been destroyed; cannot resume it")
        self.deadline = time.monotonic() + timeout
        self.retain_image()
        self.resources(require_volumes=self.state["phase"] != "allocating")
        self.subnet_check()
        if self.state["phase"] == "allocating":
            self.allocate()
        for node in self.state["nodes"]:
            self.helper(node, READ_VOLUME, readonly=True)
        self.save(phase="starting")
        self.compose("up", "-d", "--no-build", "--pull", "never", *self.services)
        if restart_nodes:
            self.event("explicit-node-restart", nodes=list(restart_nodes))
            self.compose("restart", "--timeout", str(self.state.get("shutdown_timeout", 45)), *restart_nodes)
        self.event("startup")
        with self.tools():
            self.wait(False, stable=0)
            if self.requires_baseline:
                self.establish_baseline()
                self.wait(True)
        self.save(
            phase="ready",
            baseline="complete" if self.requires_baseline else self.state["baseline"],
        )
        self.event("ready")
        return {"name": self.name, "project": self.project, "ready": True,
                "nodes": [node["node_name"] for node in self.state["nodes"]],
                "manifest_version": "1", "state": str(self.directory)}

    def status(self):
        if self.state["phase"] == "destroyed":
            return {"name": self.name, "phase": "destroyed", "ready": False}
        containers, _, _ = self.resources(require_volumes=self.state["phase"] not in ("allocating", "destroying"))
        summary = {
            container["Config"]["Labels"]["com.docker.compose.service"]: {
                "status": container["State"]["Status"], "exit_code": container["State"]["ExitCode"],
            } for container in containers
        }
        result = {"name": self.name, "phase": self.state["phase"], "containers": summary,
                  "ready": False, "state": str(self.directory)}
        if all(summary.get(node, {}).get("status") == "running" for node in self.services):
            with self.tools():
                result["service_fabric"] = self.probe()
            result["ready"] = result["service_fabric"]["ready"]
        else:
            result["problems"] = ["Not all node containers are running"]
        return result

    def down(self):
        if self.state["phase"] == "destroyed":
            raise ValueError("Cluster has already been destroyed")
        if self.state["phase"] == "destroying":
            raise ValueError("Destroy is incomplete; repeat destroy, not down")
        self.resources(require_volumes=self.state["phase"] != "allocating")
        self.compose("stop", "--timeout", str(self.state.get("shutdown_timeout", 45)))
        containers, _, _ = self.resources(require_volumes=self.state["phase"] != "allocating")
        exits = {
            container["Config"]["Labels"]["com.docker.compose.service"]: container["State"]["ExitCode"]
            for container in containers
            if container["Config"]["Labels"]["com.docker.compose.service"] in self.services
        }
        self.compose("down", "--timeout", str(self.state.get("shutdown_timeout", 45)))
        # Preserve allocating so up can finish an interrupted initial allocation.
        if self.state["phase"] != "allocating":
            self.save(phase="down")
        self.event("down", node_exit_codes=exits)
        if any(code != 0 for code in exits.values()):
            raise ValueError(
                f"Node shutdown was not clean: {exits}. Containers/network removed; all data preserved"
            )
        return {"name": self.name, "phase": self.state["phase"], "data_preserved": True}

    def destroy(self, confirmation):
        if confirmation != self.name:
            raise ValueError("Destructive action requires --confirm with the exact cluster name")
        if self.state["phase"] != "destroyed":
            _, volumes, _ = self.resources(require_volumes=False)
            retained_image = self.image_tag_id()
            self.save(phase="destroying")
            self.compose("down", "--timeout", str(self.state.get("shutdown_timeout", 45)))
            for volume in volumes:
                run(["docker", "volume", "rm", volume["Name"]])
            if retained_image:
                run(["docker", "image", "rm", self.image_tag])
            self.save(phase="destroyed")
        for name in ("ClusterManifest.xml", "compose.json", "last-status.json"):
            (self.directory / name).unlink(missing_ok=True)
        self.event("destroyed")
        return {"name": self.name, "phase": "destroyed", "data_deleted": True}


def positive_timeout(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("Timeout must be a finite positive number")
    return number


def main(*, cluster_type=Cluster, state_root=STATE_ROOT, default_subnet=DEFAULT_SUBNET,
         default_image="sf-apps-node:11.8.121.4", description=__doc__):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--state-root", type=Path, default=state_root)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("create", "up", "status", "down", "destroy"):
        action = commands.add_parser(command)
        action.add_argument("name")
        if command in ("create", "up"):
            action.add_argument("--timeout", type=positive_timeout, default=900)
        if command == "create":
            action.add_argument("--subnet", default=default_subnet,
                                help="Private IPv4 subnet (default: %(default)s)")
            action.add_argument("--image", default=default_image)
        if command == "up":
            action.add_argument("--restart-node", choices=cluster_type.services,
                                action="append", default=[])
        if command == "destroy":
            action.add_argument("--confirm", required=True)
    args = parser.parse_args()
    os.umask(0o077)
    validate_name(args.name)
    if args.command == "destroy" and args.confirm != args.name:
        raise ValueError("Destructive action requires --confirm with the exact cluster name")
    check_local_docker()
    root = args.state_root.absolute()
    with locked(root, args.name) as directory:
        cluster = cluster_type(directory, args.name)
        if args.command == "create":
            result = cluster.create(args.subnet, args.image, args.timeout)
        else:
            cluster.load()
            if args.command == "up":
                result = cluster.up(args.timeout, args.restart_node)
            elif args.command == "destroy":
                result = cluster.destroy(args.confirm)
            else:
                result = getattr(cluster, args.command)()
        print(json.dumps(result, indent=2))
        return 1 if args.command == "status" and not result["ready"] else 0


def entrypoint(**kwargs):
    try:
        return main(**kwargs)
    except subprocess.CalledProcessError as error:
        print(f"ERROR: command failed ({error.returncode}): {' '.join(map(str, error.cmd[:5]))}\n"
              f"{error.stderr or error.stdout or ''}", file=sys.stderr)
        return 1
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        print(f"ERROR: {error}. Cluster state is preserved; inspect state/last-status.json "
              "and Docker logs before retrying.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(entrypoint())
