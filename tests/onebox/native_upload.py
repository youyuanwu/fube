"""Opt-in native upload qualification; does not replace the CI uploader."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys
import urllib.error
import urllib.request
from urllib.parse import quote
import uuid

from tests.onebox.shared import runtime


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def read(allocation: runtime.Allocation, path: str) -> dict | None:
    allocation.verify()
    try:
        with urllib.request.urlopen("http://127.0.0.1:19080" + path, timeout=15) as response:
            if response.status == 204:
                return None
            body = response.read(1_048_577)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise
    require(len(body) <= 1_048_576, "native qualification response exceeds bound")
    value = json.loads(body)
    require(isinstance(value, dict), "native qualification response must be an object")
    return value


def store(allocation: runtime.Allocation, path: str) -> dict | None:
    return read(allocation, "/ImageStore/" + quote(path, safe="") + "?api-version=6.2")


def inventory(allocation: runtime.Allocation, root: str, directories: list[str]) -> dict[str, int]:
    files = {}
    for directory in directories:
        path = root + ("/" + directory if directory else "")
        value = store(allocation, path)
        require(value is not None, f"native uploaded directory missing: {path}")
        for item in value.get("StoreFiles", []):
            relative = item["StoreRelativePath"]
            require(relative.startswith(root + "/"), "native inventory escaped upload root")
            relative = relative[len(root) + 1:]
            size = int(item["FileSize"])
            require(relative not in files or files[relative] == size, "inconsistent native file size")
            files[relative] = size
    return files


def deployed_data(allocation: runtime.Allocation, directory: str, expected: dict[str, str]) -> None:
    script = r"""
import hashlib,json,sys
from pathlib import Path
root=Path(sys.argv[1]);expected=json.loads(sys.argv[2]);actual={}
for p in root.rglob("*"):
    if p.is_file() and p.name!="_.dir":
        with p.open("rb") as file:
            digest=hashlib.sha256()
            for chunk in iter(lambda:file.read(1048576),b""):
                digest.update(chunk)
            actual[p.relative_to(root).as_posix()]=digest.hexdigest()
if actual!=expected:
    raise RuntimeError("deployed Data package contents differ from upload source")
print("Deployed Data file hashes match upload source.")
"""
    allocation.execute("python3", "-c", script, directory, json.dumps(expected), timeout=60)


def qualify(args: argparse.Namespace) -> None:
    lines = args.owner_file.read_text(encoding="utf-8").splitlines()
    require(len(lines) == 1 and lines[0].startswith("owner="), "original owner file is invalid")
    allocation = runtime.Allocation(args.name, lines[0].removeprefix("owner="))
    for executable in (args.cli, args.app, args.observer, args.native_oracle):
        require(executable.is_file() and not executable.is_symlink(),
                f"missing prebuilt executable: {executable}")
    allocation.sf("cluster", "select", "--endpoint", "http://127.0.0.1:19080")
    run_id = uuid.uuid4().hex
    source, type_name, version = runtime.package(run_id, args.app.resolve())
    remote = "/tmp/fube-native-upload-" + run_id
    application = "fabric:/Upload" + run_id
    app_id = application.removeprefix("fabric:/")
    remote_created = provision_attempted = create_attempted = False
    upload_attempted = False
    try:
        data = source / "RustRuntimeService/Data"
        nested = data / "nested space-\u7a7a"
        nested.mkdir()
        (nested / "empty.bin").write_bytes(b"")
        (data / "empty-directory").mkdir()
        (data / "large.bin").write_bytes(b"0123456789abcdef" * (7 * 1024 * 1024 // 16))
        (source / "_.dir").write_bytes(b"must be regenerated, not copied")
        obsolete = data / "obsolete.bin"
        obsolete.write_bytes(b"obsolete")
        directories = [""] + sorted(p.relative_to(source).as_posix()
                                     for p in source.rglob("*") if p.is_dir())
        require(store(allocation, run_id) is None, "unique native upload path collision")
        require(read(allocation, f"/Applications/{app_id}?api-version=6.0") is None,
                "unique application collision")
        allocation.execute("mkdir", "-m", "700", remote)
        remote_created = True
        cid = allocation.verify()
        runtime.run("docker", "cp", str(args.native_oracle.resolve()), f"{cid}:{remote}/oracle")
        runtime.run("docker", "cp", str(source), f"{cid}:{remote}/package", timeout=120)
        upload_attempted = True
        allocation.execute(
            "timeout", "--signal=TERM", "--kill-after=5s", "300s",
            "env", "LD_LIBRARY_PATH=/opt/microsoft/servicefabric/bin/Fabric/Fabric.Code",
            remote + "/oracle", "127.0.0.1:19000", remote + "/package", run_id, timeout=310,
        )
        before = inventory(allocation, run_id, directories)
        require("RustRuntimeService/Data/obsolete.bin" in before,
                "native SDK predecessor upload did not contain stale file")
        obsolete.unlink()
        native = dict(before)
        del native["RustRuntimeService/Data/obsolete.bin"]
        allocation.verify()
        runtime.run(
            str(args.cli.resolve()), "--endpoint", "127.0.0.1:19000", "--unsecured",
            "application", "upload", "--path", str(source), "--timeout", "300", timeout=310,
        )
        uploaded = inventory(allocation, run_id, directories)
        require(uploaded == native, "Rust upload inventory differs from native SDK copy")
        require("RustRuntimeService/Data/obsolete.bin" not in uploaded,
                "AtomicCopy did not remove stale destination content")
        expected_files = {p.relative_to(source).as_posix(): p.stat().st_size
                          for p in source.rglob("*") if p.is_file() and p.name != "_.dir"}
        expected_files.update({(directory + "/" if directory else "") + "_.dir": 0
                               for directory in directories})
        require(uploaded == expected_files, "native package contents/completion markers differ")
        expected_data = {p.relative_to(data).as_posix(): runtime.digest(p)
                         for p in data.rglob("*") if p.is_file()}
        provision_attempted = True
        allocation.sf("application", "provision", "--application-type-build-path", run_id,
                      "--timeout", "180", timeout=190)
        create_attempted = True
        allocation.sf("application", "create", "--app-name", application, "--app-type", type_name,
                      "--app-version", version, "--timeout", "180", timeout=190)
        observation = runtime.wait_observe(application + "/Singleton", observer=args.observer.resolve())
        require(observation["rpc"]["packages"]["configuration"]["value"] == "before-upgrade",
                "uploaded configuration was not activated")
        deployed_data(allocation, observation["rpc"]["packages"]["data"]["path"], expected_data)
    finally:
        primary = sys.exception()
        errors = []
        actions = []
        if create_attempted:
            def delete_application():
                value = read(allocation, f"/Applications/{app_id}?api-version=6.0")
                if value is not None:
                    require(value.get("Name") == application and value.get("TypeName") == type_name
                            and value.get("TypeVersion") == version,
                            "application identity changed; retain")
                    allocation.sf("application", "delete", "--application-id", app_id,
                                  "--timeout", "180", timeout=190)
                    require(read(allocation, f"/Applications/{app_id}?api-version=6.0") is None,
                            "application cleanup unconfirmed")
            actions.append(delete_application)
        if provision_attempted:
            def unprovision():
                value = read(allocation, "/ApplicationTypes/" + quote(type_name, safe="")
                             + "?api-version=6.0")
                if value is not None:
                    require(isinstance(value.get("Items"), list), "invalid native type-list response")
                    if value["Items"]:
                        require(all(item.get("Version") == version for item in value["Items"]),
                                "application type identity changed; retain")
                        allocation.sf("application", "unprovision", "--application-type-name", type_name,
                                      "--application-type-version", version, "--timeout", "180", timeout=190)
                        after = read(allocation, "/ApplicationTypes/" + quote(type_name, safe="")
                                     + "?api-version=6.0")
                        require(after is None or after.get("Items") == [],
                                "application type cleanup unconfirmed")
            actions.append(unprovision)
        if upload_attempted:
            def delete_upload():
                if store(allocation, run_id) is not None:
                    allocation.sf("store", "delete", "--content-path", run_id, "--timeout", "120",
                                  timeout=130)
                require(store(allocation, run_id) is None, "ImageStore cleanup unconfirmed")
            actions.append(delete_upload)
        if remote_created:
            actions.append(lambda: allocation.execute("rm", "-r", "--", remote))
        actions.append(lambda: shutil.rmtree(source))
        for action in actions:
            try:
                action()
            except Exception as error:
                errors.append(error)
        if errors:
            if primary is not None:
                raise BaseExceptionGroup("native upload and cleanup failed", [primary, *errors])
            raise ExceptionGroup("native upload cleanup failed", errors)
    print("PASS: native SDK layout/replacement, empty/multi-chunk content, markers, "
          "CLI upload, provisioning, live application and verified cleanup.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True)
    parser.add_argument("--owner-file", type=Path, required=True)
    parser.add_argument("--cli", type=Path, required=True)
    parser.add_argument("--app", type=Path, required=True)
    parser.add_argument("--observer", type=Path, required=True)
    parser.add_argument("--native-oracle", type=Path, required=True)
    qualify(parser.parse_args())


if __name__ == "__main__":
    main()
