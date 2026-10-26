# Ubuntu single-container onebox

This is an **experimental, unsecured, single-host development cluster** running
five Service Fabric nodes (`N0010` through `N0050`) in **one privileged Ubuntu
22.04 container**. It uses Service Fabric `11.8.121.4`, `IsScaleMin=true`,
one private Docker bridge, and one retained named data volume. Administrative
commands run inside the same container; no separate tools container, SSH server,
development container, Rust toolchain, or sibling checkout is required.

The topology and endpoint assignments are ported from
`service-fabric-rs/.devcontainer/onebox/` at source revision
`7ad146aea4dd2d23b3886121c32175e75e024b06`. The destructive shell bootstrap is
replaced by the same guarded lifecycle used for the
[multi-container cluster](LinuxContainerCluster.md). The two workflows share
resource ownership checks, image retention, readiness checks, shutdown handling,
and account/tracing setup. Unlike the multi-container cluster, onebox does not
provision or upgrade a native Cluster Manager baseline.

## Build and create

Use a Linux x86-64 host with Python 3.10+, `ip` from iproute2, Docker Engine, and
Docker Compose v2+. The Docker daemon must be local and support privileged Linux
containers. `just` is optional. This workflow does not install Service Fabric on
the host.

```sh
just onebox-build
just onebox create dev

# Without just, from the repository root:
docker build --tag sf-apps-onebox:11.8.121.4 \
  --file docker/onebox/Dockerfile docker
python3 scripts/onebox_ctl.py create dev
```

Use **`docker` as the build context**, not `docker/onebox`: the Dockerfile copies
the shared bootstrap and probe from `docker/linux-cluster`. The build is
self-contained, downloads the public Microsoft runtime package, and accepts its
EULA. The Ubuntu digest, SF package/checksum, Fabric executable checksum, and
sfctl version are pinned identically to the multi-container image. Dependency
repositories and transitive Python dependencies are not snapshot-pinned.

Creation chooses bridge address `172.30.91.11` from the default
`172.30.91.0/24` subnet. Pass `--subnet CIDR` on `create` if that subnet overlaps
another route or Docker network. Overlaps are rejected, never silently reassigned.
The subnet must be private IPv4 with at least 16 addresses.

The only published ports are loopback-bound:

| Host endpoint | Purpose |
| --- | --- |
| `http://127.0.0.1:19080` | HTTP management gateway; Explorer at `/Explorer/index.html` |
| `127.0.0.1:19000` | Native FabricClient TCP gateway |

Keep these mappings loopback-only: the cluster has **no authentication or TLS**.
Stop any other cluster or process using either host port before starting this
onebox. A different subnet does not avoid a host-port collision. In WSL with
localhost forwarding, Windows can also reach these published localhost ports.
Other node gateways and application/service endpoints are not published; use
the Linux host or an attached container for private bridge endpoints. Publishing
the native gateway does not make every resolved service address reachable from
Windows or another host.

All five nodes share one IP and use distinct ports:

| Node | Native client | HTTP gateway | Application port range |
| --- | --- | --- | --- |
| N0010 | 19000 | 19080 | 22001-23000 |
| N0020 | 19010 | 19082 | 23001-24000 |
| N0030 | 19020 | 19084 | 24001-25000 |
| N0040 | 19030 | 19086 | 25001-26000 |
| N0050 | 19040 | 19088 | 26001-27000 |

All are seed nodes, with distinct development fault/upgrade domains. These are
logical domains on **one physical failure boundary**, not availability isolation.
System stateful services use three active replicas across the five nodes, not five
active replicas; additional healthy standby replicas can remain after recovery.
As in the source onebox, minimum replica-set size is one for local
development; this is not a production configuration.

`create` freezes and privately tags the immutable local image ID, generates new
file-store credentials, and deploys all five nodes. Like the source onebox, it
**does not upload/provision runtime packages or submit a baseline upgrade**.
There is no fixed 30-second stability wait: creation returns on the first healthy
observation of every HTTP gateway, the recorded manifest, five Up/enabled/Ok
nodes at code/configuration `11.8.121.4 / 1`, and healthy three-active-replica system
partitions. A running Docker container or an open port alone is not success.
No sample app is deployed implicitly.

Native cluster-upgrade progress may retain its bootstrap placeholder
(`0.0.0.0`, empty configuration, `Invalid` upgrade mode). That is expected for
this profile: readiness checks the actual node/runtime and manifest versions,
not completion of an unnecessary upgrade. `up` uses the same fast readiness path;
it does not add a baseline to a retained cluster or remove a baseline established
by an older controller. Full native recovery can still take several minutes
after a complete retained shutdown.

The source script's default startup deletes existing cluster data. This
controller deliberately preserves it on `up`. For an intentionally fresh,
disposable instance, use confirmed `destroy` followed by `create`; deleting state
is never an automatic startup optimization.

## Lifecycle

The command structure matches `just cluster`:

```sh
just onebox status dev
just onebox down dev
just onebox up dev
just onebox destroy dev --confirm dev

# Equivalent entry point:
python3 scripts/onebox_ctl.py status dev
```

| Command | Behavior |
| --- | --- |
| `create NAME [--subnet CIDR] [--image IMAGE] [--timeout SECONDS]` | Allocate a new five-node onebox. Reject existing state or colliding resources. |
| `up NAME [--timeout SECONDS]` | Recreate/start the container with the same image, address, volume, and current native deployment; wait for actual readiness. |
| `status NAME` | JSON Docker and live SF observations. Never starts a stopped container. Exit 0 means healthy; exit 1 means stopped, unhealthy, missing, or conflicting resources. |
| `down NAME` | Stop/remove the container and bridge, preserving the entire five-node volume and generated configuration. Report unclean shutdown even though teardown preserves data. |
| `destroy NAME --confirm NAME` | Remove only this allocation's owned container, bridge, volume, generated secrets, and private image tag. Exact-name confirmation is required. |

Names and resources are separate from the multi-container workflow:
`sf-onebox-NAME` versus `sf-linux-NAME`. A lifecycle command never adopts
another project's resources. The default subnets differ, but both workflows
publish HTTP port 19080, so their default configurations cannot run concurrently.

State is stored privately under ignored `build/oneboxes/NAME/`, including
`state.json`, frozen `compose.json`, `ClusterManifest.xml`, `events.jsonl`, and
`last-status.json`. The data volume is `sf-onebox-NAME_data`, mounted at
`/var/lib/sf-node`. Keep the state directory alongside the volume: deleting the
directory is not teardown and prevents safe resumption. Use `--state-root PATH`
**before** the subcommand for another private, ignored location.

Operations serialize with a per-name filesystem lock. Missing volumes, changed
image/account identities, edited frozen inputs, and interrupted deployments are
errors, not permission to bootstrap over retained data. Retained starts do not
rerun fresh `Create` or regenerate credentials. Do not prune the retained image
while the onebox is down. `destroy` leaves a small tombstone and operation log,
supports resumption after interrupted deletion, and allows later `create` with
new identities and credentials.

## Inspection and recovery

`create` and `up` have a finite 900-second default readiness budget; override with
`--timeout SECONDS`. Failure leaves state and resources for investigation, never
silently resets data. Inspect `last-status.json`, `events.jsonl`, and container
logs:

```sh
docker compose -f build/oneboxes/dev/compose.json logs --tail 100 onebox
python3 scripts/linux_cluster_diagnostics.py dev --onebox \
  --output build/oneboxes/dev/diagnostics.json
just onebox up dev --restart-node onebox
```

The explicit restart affects the **entire five-node container**, not an individual
SF node. It retains data and waits for full recovery. It is never an automatic
fallback for failed readiness.

An interrupted initial deployment (`configuring` identity) or pending native
`TargetInformation.xml` requires investigation. Onebox never submits baseline
upgrades, including when resuming an allocation created by an older controller.
An interrupted upgrade from that older workflow still requires inspection; the
fast-start path does not undo it or reset its journal. Use
`destroy`/`create` only when intentionally discarding a disposable allocation.
No global Docker prune or force-volume deletion is part of this workflow.

This profile provides the native clear-TCP gateway needed for initial Rust
FabricClient interoperability work. It **does not qualify TLS or claims
authentication**; those require a separately secured native environment.
Production security, runtime upgrades, dynamic node joins, and Windows/Azure
Linux images are outside this port.

## Continuous integration

The Linux lifecycle matrix in `.github/workflows/ci.yaml` independently builds and
runs the multi-container cluster and this single-container onebox on Ubuntu
24.04 hosts. Each entry exercises creation, health, retained down/up, resumed
health, and survival of a synthetic naming-store entry. Only the multi-container
entry establishes a native baseline; onebox uses source-style startup without
upgrades or a fixed stability wait. Each entry always attempts allowlisted
credential-safe diagnostics and exact-name confirmed teardown. XML, raw runtime logs,
environment variables, and arbitrary health/error text are not uploaded.

Run the offline controller tests without Docker:

```sh
python3 -m unittest discover -s scripts -p "test_linux*.py"
```

Onebox tests also check five-node port separation, all-five-gateway readiness,
three-replica placement, a single retained volume, absence of baseline submission
and artificial stabilization waits, and absence of an auxiliary tools container.
Linux-only tests skip during Windows test discovery.
