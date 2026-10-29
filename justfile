build: build-rust

build-rust:
    cargo build --workspace

onebox-build:
    docker build --tag sf-apps-onebox:11.8.121.4 --file "{{join(justfile_directory(), 'docker', 'onebox', 'Dockerfile')}}" "{{join(justfile_directory(), 'docker')}}"

onebox +args:
    python3 "{{join(justfile_directory(), 'scripts', 'onebox_ctl.py')}}" {{args}}
