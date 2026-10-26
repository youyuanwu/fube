set windows-shell := ["powershell.exe", "-NoLogo", "-NoProfile", "-Command"]

build: build-rust package

build-rust:
    cargo build --workspace

package:
    python "{{join(justfile_directory(), 'scripts', 'package_kvmap.py')}}"

cluster-build:
    docker build --tag sf-apps-node:11.8.121.4 "{{join(justfile_directory(), 'docker', 'linux-cluster')}}"

cluster +args:
    python3 "{{join(justfile_directory(), 'scripts', 'linux_cluster_ctl.py')}}" {{args}}

onebox-build:
    docker build --tag sf-apps-onebox:11.8.121.4 --file "{{join(justfile_directory(), 'docker', 'onebox', 'Dockerfile')}}" "{{join(justfile_directory(), 'docker')}}"

onebox +args:
    python3 "{{join(justfile_directory(), 'scripts', 'onebox_ctl.py')}}" {{args}}
