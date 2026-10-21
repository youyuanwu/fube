"""Named Ubuntu oneboxes: five Service Fabric nodes in one retained container."""

import re
import secrets
from string import Template
import sys

import linux_cluster_ctl as ctl
from linux_cluster import check_subnet


STATE_ROOT = ctl.REPO / "build/oneboxes"
DEFAULT_SUBNET = "172.30.91.0/24"
TEMPLATE = ctl.REPO / "docker/onebox/ClusterManifest.template.xml"
NODES = ("N0010", "N0020", "N0030", "N0040", "N0050")


def generate(project, subnet, image_id, pinned_image, manifest_path):
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", project):
        raise ValueError("Invalid Compose project name")
    network = check_subnet(subnet, [])
    address = str(network[11])
    manifest = Template(TEMPLATE.read_text()).substitute(
        CLUSTER_NAME=project, NODE_IP=address,
        FSS_PRIMARY_SECRET=secrets.token_hex(32),
        FSS_SECONDARY_SECRET=secrets.token_hex(32),
    )
    identity = {
        "cluster_id": project, "node_name": "onebox", "node_ip": address,
        "image_id": image_id, "runtime_version": ctl.RUNTIME,
        "volume_name": project + "_data", "layout": "onebox", "phase": "allocated",
    }
    compose = {
        "name": project,
        "services": {"onebox": {
            "image": pinned_image, "hostname": "onebox", "privileged": True,
            "stop_grace_period": "45s",
            "environment": {
                "SF_" + key.upper(): value for key, value in identity.items() if key != "phase"
            },
            "volumes": [
                "data:/var/lib/sf-node",
                {"type": "bind", "source": str(manifest_path),
                 "target": "/run/sf-bootstrap/ClusterManifest.xml", "read_only": True},
            ],
            "networks": {"default": {"ipv4_address": address}},
        }},
        "volumes": {"data": {}},
        "networks": {"default": {"ipam": {"config": [{"subnet": str(network)}]}}},
    }
    return compose, {"nodes": [identity]}, manifest


def definition(state, directory):
    compose, nodes = ctl.definition(state, directory, generate, "onebox")
    if state.get("publish_gateway", False):
        compose["services"]["onebox"]["ports"].append("127.0.0.1:19000:19000")
    return compose, nodes


class Onebox(ctl.Cluster):
    project_prefix = "sf-onebox-"
    image_repository = "sf-apps-onebox"
    image_label = "io.github.service-fabric-apps.onebox"
    services = ("onebox",)
    admin_service = "onebox"
    requires_baseline = False
    generate = staticmethod(generate)
    definition = staticmethod(definition)

    def up(self, timeout, restart_nodes=()):
        result = super().up(timeout, restart_nodes)
        result.update(
            nodes=list(NODES), http_gateway="http://127.0.0.1:19080",
            client_endpoint="127.0.0.1:19000",
        )
        return result


def main():
    return ctl.entrypoint(
        cluster_type=Onebox, state_root=STATE_ROOT, default_subnet=DEFAULT_SUBNET,
        default_image="sf-apps-onebox:" + ctl.RUNTIME, description=__doc__,
    )


if __name__ == "__main__":
    sys.exit(main())
