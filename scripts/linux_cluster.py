import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
from string import Template
import subprocess
import sys


TEMPLATE = Path(__file__).resolve().parents[1] / "docker/linux-cluster/ClusterManifest.template.xml"


def check_subnet(subnet, occupied):
    network = ipaddress.IPv4Network(subnet)
    private_ranges = ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
    if network.num_addresses < 16 or not any(
        network.subnet_of(ipaddress.IPv4Network(private)) for private in private_ranges
    ):
        raise ValueError("Use a private IPv4 subnet with at least 16 addresses")
    for address in occupied:
        other = ipaddress.ip_network(address, strict=False)
        if other.version == 4 and network.overlaps(other):
            raise ValueError(f"Subnet overlaps existing route/network: {address}")
    return network


def generate(project, subnet, image_id, pinned_image, manifest_path):
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", project):
        raise ValueError("Invalid Compose project name")
    network = check_subnet(subnet, [])
    manifest = Template(TEMPLATE.read_text()).substitute(
        CLUSTER_NAME=project,
        N0_IP=str(network[11]),
        N1_IP=str(network[12]),
        N2_IP=str(network[13]),
        FSS_PRIMARY_SECRET=secrets.token_hex(32),
        FSS_SECONDARY_SECRET=secrets.token_hex(32),
    )
    services = {}
    identities = []
    volumes = {}
    for index in range(3):
        name = f"n{index}"
        volume = f"{name}-data"
        identity = {
            "cluster_id": project,
            "node_name": name,
            "node_ip": str(network[11 + index]),
            "image_id": image_id,
            "runtime_version": "11.8.121.4",
            "volume_name": f"{project}_{volume}",
            "phase": "allocated",
        }
        identities.append(identity)
        volumes[volume] = {}
        services[name] = {
            "image": pinned_image,
            "hostname": name,
            "privileged": True,
            "stop_grace_period": "45s",
            "environment": {
                "SF_" + key.upper(): value
                for key, value in identity.items()
                if key != "phase"
            },
            "volumes": [
                f"{volume}:/var/lib/sf-node",
                {"type": "bind", "source": str(manifest_path),
                 "target": "/run/sf-bootstrap/ClusterManifest.xml", "read_only": True},
            ],
            "networks": {"default": {"ipv4_address": identity["node_ip"]}},
            "labels": {"com.service-fabric-apps.validation": "g1"},
        }
    services["tools"] = {
        "image": pinned_image,
        "init": True,
        "entrypoint": ["sleep", "infinity"],
        "networks": {"default": {"ipv4_address": str(network[10])}},
    }
    compose = {
        "name": project, "services": services, "volumes": volumes,
        "networks": {"default": {"ipam": {"config": [{"subnet": str(network)}]}}},
    }
    inventory = {
        "stage": "G1", "project": project, "image_id": image_id,
        "image": pinned_image, "subnet": str(network), "seeds": ["n0", "n1", "n2"],
        "nodes": identities, "manifest_version": "1",
        "manifest_sha256": hashlib.sha256(manifest.encode()).hexdigest(),
        "ssh_enabled": False,
    }
    return compose, inventory, manifest


def output(arguments):
    return subprocess.check_output(arguments, text=True, timeout=30)


def prepare(project, subnet, image, destination):
    destination = destination.resolve()
    if destination.exists():
        raise ValueError("Output directory already exists; use a fresh validation directory")
    routes = json.loads(output(["ip", "-j", "-4", "route"]))
    network_ids = output(["docker", "network", "ls", "-q"]).split()
    networks = json.loads(output(["docker", "network", "inspect", *network_ids]))
    occupied = [route["dst"] for route in routes if route.get("dst", "default") != "default"]
    occupied += [
        config["Subnet"] for network in networks
        for config in network.get("IPAM", {}).get("Config") or [] if config.get("Subnet")
    ]
    check_subnet(subnet, occupied)
    image_data = json.loads(output(["docker", "image", "inspect", image]))[0]
    pinned_image = (image_data.get("RepoDigests") or [image_data["Id"]])[0]
    compose, inventory, manifest = generate(
        project, subnet, image_data["Id"], pinned_image,
        destination / "ClusterManifest.xml",
    )
    existing = set(output(["docker", "volume", "ls", "--format", "{{.Name}}"]).split())
    if existing.intersection(node["volume_name"] for node in inventory["nodes"]):
        raise ValueError("Node volume name already exists; refusing implicit identity reuse")
    if output(["docker", "ps", "-a", "-q",
               "--filter", "label=com.docker.compose.project=" + project]).strip():
        raise ValueError("Compose project already has containers")
    destination.mkdir(parents=True)
    with open(
        destination / "ClusterManifest.xml",
        "x", opener=lambda path, flags: os.open(path, flags, 0o600)
    ) as manifest_file:
        manifest_file.write(manifest)
    for name, data in [("compose", compose), ("inventory", inventory)]:
        (destination / f"{name}.json").write_text(json.dumps(data, indent=2) + "\n")
    subprocess.run(
        ["docker", "compose", "-f", str(destination / "compose.json"), "create", "--no-build"],
        check=True, timeout=120,
    )
    seed = (
        "import json, os, pathlib, sys\n"
        "p = pathlib.Path('/var/lib/sf-node')\n"
        "if any(p.iterdir()):\n"
        "    raise SystemExit('Volume is not empty; refusing identity reuse')\n"
        "with (p / 'identity.json').open('x') as f:\n"
        "    json.dump(json.load(sys.stdin), f)\n"
        "    f.flush()\n"
        "    os.fsync(f.fileno())\n"
    )
    for identity in inventory["nodes"]:
        subprocess.run(
            ["docker", "run", "--rm", "-i", "--network", "none", "--entrypoint", "python3",
             "--mount", f"type=volume,src={identity['volume_name']},dst=/var/lib/sf-node",
             inventory["image"], "-c", seed],
            input=json.dumps(identity), text=True, check=True, timeout=30,
        )
    print(f"Prepared {project}; start with docker compose -f {destination / 'compose.json'} up -d")


def main():
    parser = argparse.ArgumentParser(
        description="Prepare an isolated experimental G1 static harness; not a join controller."
    )
    parser.add_argument("--project", required=True)
    parser.add_argument("--subnet", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    prepare(args.project, args.subnet, args.image, args.output)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print(f"ERROR: static harness preparation failed: {error}", file=sys.stderr)
        sys.exit(1)
