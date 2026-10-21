"""Read-only SF observations for the local container-cluster controller."""

import json
from pathlib import Path
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET


PARTITIONS = {
    "ClusterManager": "00000000-0000-0000-0000-000000002000",
    "FailoverManager": "00000000-0000-0000-0000-000000000001",
    "ImageStore": "00000000-0000-0000-0000-000000003000",
    "Naming": "00000000-0000-0000-0000-000000001000",
}
NAMESPACE = {"sf": "http://schemas.microsoft.com/2011/01/fabric"}


def expected_members(xml):
    manifest = ET.fromstring(xml)
    types = {
        node_type.get("Name"): node_type
        for node_type in manifest.findall("sf:NodeTypes/sf:NodeType", NAMESPACE)
    }
    members = {}
    for node in manifest.findall("sf:Infrastructure/sf:Linux/sf:NodeList/sf:Node", NAMESPACE):
        node_type = types[node.get("NodeTypeRef")]
        port = node_type.find("sf:Endpoints/sf:HttpGatewayEndpoint", NAMESPACE).get("Port")
        members[node.get("NodeName")] = {
            "node_ip": node.get("IPAddressOrFQDN"),
            "gateway": node.get("IPAddressOrFQDN") + (":" + port if port != "19080" else ""),
            "node_type": node.get("NodeTypeRef"),
            "seed": node.get("IsSeedNode", "false") in ("true", "1"),
            "upgrade_domain": node.get("UpgradeDomain", ""),
            "fault_domain": node.get("FaultDomain", ""),
        }
    if not members:
        raise ValueError("Manifest contains no Linux nodes")
    return members


def semantic_manifest(xml):
    def normalize(element):
        attributes = dict(element.attrib)
        local = element.tag.rsplit("}", 1)[-1]
        # These false values are schema defaults omitted by the runtime.
        for tag, attribute in (("Linux", "IsScaleMin"), ("Node", "IsSeedNode")):
            if local == tag and attributes.get(attribute, "false") in ("false", "0"):
                attributes.pop(attribute, None)
        return (
            element.tag, tuple(sorted(attributes.items())), (element.text or "").strip(),
            tuple(sorted(normalize(child) for child in element)),
        )
    return normalize(ET.fromstring(xml))


def get(address, path, deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Probe deadline exceeded")
    authority = address if ":" in address else address + ":19080"
    request = urllib.request.Request(f"http://{authority}{path}?api-version=6.0")
    # Development bridge traffic must not go through a host's HTTP proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=min(3, remaining)) as response:
        value = json.load(response)
    if not isinstance(value, dict):
        raise ValueError("Invalid SF object response")
    return value


def items(response):
    if not isinstance(response, dict) or not isinstance(response.get("Items"), list):
        raise ValueError("Invalid SF list response")
    if response.get("ContinuationToken"):
        raise ValueError("Unexpected SF pagination; refusing incomplete observations")
    if any(not isinstance(item, dict) for item in response["Items"]):
        raise ValueError("Invalid SF list record")
    return response["Items"]


def baseline_state(upgrade, runtime, config):
    if not isinstance(upgrade, dict):
        raise ValueError("Invalid upgrade response")
    state = upgrade.get("UpgradeState")
    target = upgrade.get("CodeVersion") == runtime and upgrade.get("ConfigVersion") == config
    if target and state == "RollingForwardCompleted" and upgrade.get("RollingUpgradeMode") == "Monitored":
        return "complete"
    if target and state in ("RollingForwardPending", "RollingForwardInProgress") \
            and upgrade.get("RollingUpgradeMode") == "Monitored":
        return "active"
    if state == "RollingForwardCompleted" and upgrade.get("CodeVersion") == "0.0.0.0" \
            and upgrade.get("ConfigVersion") == "" and upgrade.get("RollingUpgradeMode") == "Invalid":
        return "uninitialized"
    raise ValueError(
        f"Unexpected native upgrade: {state}, "
        f"code={upgrade.get('CodeVersion')}, config={upgrade.get('ConfigVersion')}"
    )


def snapshot(spec, expected_xml, baseline_required=True, budget=45):
    deadline = time.monotonic() + budget
    expected = expected_members(expected_xml)
    result = {"gateways": {}, "replicas": {}, "problems": []}
    problems = result["problems"]
    canonical = semantic_manifest(expected_xml)
    for name, local in expected.items():
        gateway = {}
        result["gateways"][name] = gateway
        try:
            address = local["gateway"]
            gateway["nodes"] = get(address, "/Nodes", deadline)
            gateway["health"] = get(address, "/$/GetClusterHealth", deadline)
            gateway["upgrade"] = get(address, "/$/GetUpgradeProgress", deadline)
            manifest = get(address, "/$/GetClusterManifest", deadline)
            if not isinstance(manifest, dict) or not isinstance(manifest.get("Manifest"), str):
                raise ValueError("Invalid manifest response")
            if semantic_manifest(manifest["Manifest"]) != canonical:
                problems.append(f"{name}: active XML differs from the recorded manifest")
            members = items(gateway["nodes"])
            if any(not isinstance(node, dict) or not isinstance(node.get("Name"), str)
                   for node in members):
                raise ValueError("Invalid SF node record")
            if len(members) != len(expected) or {node["Name"] for node in members} != set(expected):
                problems.append(f"{name}: unexpected cluster membership")
            for member in members:
                wanted = expected.get(member["Name"])
                if wanted is None or any((
                    member.get("NodeStatus") != "Up",
                    member.get("HealthState") != "Ok",
                    member.get("IpAddressOrFQDN") != wanted["node_ip"],
                    member.get("CodeVersion") != spec["runtime_version"],
                    member.get("ConfigVersion") != spec["manifest_version"],
                    member.get("IsSeedNode") is not wanted["seed"],
                    member.get("Type") != wanted["node_type"],
                    member.get("UpgradeDomain", "") != wanted["upgrade_domain"],
                    member.get("FaultDomain", "") != wanted["fault_domain"],
                    member.get("NodeDeactivationInfo", {}).get("NodeDeactivationStatus") != "None",
                )):
                    problems.append(f"{name}: node {member['Name']} is not Up/enabled/Ok at target")
            if gateway["health"].get("AggregatedHealthState") != "Ok":
                problems.append(f"{name}: cluster health is not Ok")
            gateway["baseline"] = baseline_state(
                gateway["upgrade"], spec["runtime_version"], spec["manifest_version"]
            )
            if baseline_required and gateway["baseline"] != "complete":
                problems.append(f"{name}: native baseline is not complete")
        except (OSError, ValueError, ET.ParseError) as error:
            gateway["error"] = str(error)
            problems.append(f"{name}: {error}")
    primary_gateway = next(iter(expected.values()))["gateway"]
    for name, partition in PARTITIONS.items():
        try:
            replicas = get(primary_gateway, f"/Partitions/{partition}/$/GetReplicas", deadline)
            result["replicas"][name] = replicas
            members = items(replicas)
            replica_nodes = {replica.get("NodeName") for replica in members}
            active = [replica for replica in members
                      if replica.get("ReplicaRole") in ("Primary", "ActiveSecondary")]
            # Five-node recovery can retain healthy standby replicas outside the active set.
            if len(active) != 3 or len(replica_nodes) != len(members) \
                    or not replica_nodes.issubset(expected) \
                    or any(replica.get("ReplicaStatus") != "Ready" for replica in active) \
                    or any(replica.get("HealthState") != "Ok" for replica in members) \
                    or any(replica.get("ReplicaRole") != "IdleSecondary"
                           or replica.get("ReplicaStatus") != "Standby"
                           for replica in members if replica not in active) \
                    or sum(replica.get("ReplicaRole") == "Primary" for replica in active) != 1:
                problems.append(f"{name}: expected three Ready replicas and one primary")
        except (OSError, ValueError) as error:
            problems.append(f"{name}: {error}")
    try:
        applications = get(primary_gateway, "/Applications", deadline)
        result["applications"] = applications
        if any(app.get("Status") != "Ready" or app.get("HealthState") != "Ok"
               for app in items(applications)):
            problems.append("Existing applications are not Ready/Ok")
    except (OSError, ValueError) as error:
        problems.append(f"Applications: {error}")
    result["ready"] = not problems
    return result


if __name__ == "__main__":
    arguments = json.load(sys.stdin)
    print(json.dumps(snapshot(
        arguments["spec"],
        Path("/run/sf-bootstrap/ClusterManifest.xml").read_text(),
        arguments.get("baseline_required", True),
        arguments.get("budget", 45),
    )))
