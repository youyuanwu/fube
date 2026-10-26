"""Prepare or execute the pinned clusterless native-runtime test profile.

No cluster services are installed/started. OS ABI libraries must already be
available in the loader or the explicitly selected private dependency directory.
"""

import argparse
import hashlib
import pathlib
import shutil
import subprocess
import sys
import tempfile
import urllib.request

VERSION = "11.8.121.4"
PACKAGE_SHA256 = "fbcb0b8d52e84b7e2bf9c336d9e0ece8cb9ff7fa1de53367d56c6ff8ab850c83"
PACKAGE_URL = (
    "https://download.microsoft.com/download/3/1/F/"
    "31F3FEEB-F073-4E27-A98B-8E691FF74F40/ServiceFabric.U22.11.8.121.4.deb"
)
LIBRARY_HASHES = {
    "libFabricRuntime.so": "9ce2cac38006b12c8a6b4ef5735f0ab9d8e9ab29f09a7a414e2dc5b329c5046f",
    "libFabricCommon.so": "650ea380d8a892d9015c7159ed09c2b23935b3a0f529c7f3e4b6cf9a2a82414f",
}
RELATIVE_NATIVE = pathlib.Path("opt/microsoft/servicefabric/bin/Fabric/Fabric.Code")
DIAGNOSTIC_LIMIT = 1024 * 1024
SCENARIOS = ("bootstrap", "close", "abort", "manifest-close", "manifest-abort")


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def prepare(root):
    root.mkdir(parents=True, exist_ok=True)
    package = root / f"servicefabric_{VERSION}.deb"
    if not package.exists():
        with urllib.request.urlopen(PACKAGE_URL, timeout=60) as source:
            # Exclusive creation: never overwrite a user-supplied archive.
            with package.open("xb") as target:
                while block := source.read(1024 * 1024):
                    target.write(block)
    if digest(package) != PACKAGE_SHA256:
        raise ValueError(f"native package hash mismatch: {package}")
    destination = root / "runtime"
    if not destination.exists():
        subprocess.run(["dpkg-deb", "--extract", str(package), str(destination)],
                       check=True, timeout=120)
    return destination / RELATIVE_NATIVE


def qualify_libraries(native):
    for name, expected in LIBRARY_HASHES.items():
        if digest(native / name) != expected:
            raise ValueError(f"unsupported native library hash: {name}")
    if (native / "ClusterVersion").read_text().strip() != VERSION:
        raise ValueError(f"require native build {VERSION}")


def save_diagnostics(directory, scenario, output):
    if scenario not in SCENARIOS:
        raise ValueError(f"unsupported diagnostic scenario: {scenario}")
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{scenario}.log").write_bytes(output[-DIAGNOSTIC_LIMIT:])
    report = directory / f"{scenario}.json"
    report.unlink(missing_ok=True)
    for line in reversed(output.splitlines()):
        if line.startswith(b"MSSF_REPORT "):
            payload = line.removeprefix(b"MSSF_REPORT ")
            if len(payload) + 1 > DIAGNOSTIC_LIMIT:
                raise ValueError("native report exceeds diagnostic size limit")
            report.write_bytes(payload + b"\n")
            break


def run_host(command, repo, diagnostics, scenario):
    if diagnostics is None:
        return subprocess.run(command, cwd=repo, timeout=140, check=False).returncode
    try:
        result = subprocess.run(command, cwd=repo, timeout=140, check=False,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except subprocess.TimeoutExpired as error:
        output = error.stdout or b""
        save_diagnostics(diagnostics, scenario, output)
        print(output[-DIAGNOSTIC_LIMIT:].decode("utf-8", errors="replace"), end="")
        raise
    save_diagnostics(diagnostics, scenario, result.stdout)
    print(result.stdout[-DIAGNOSTIC_LIMIT:].decode("utf-8", errors="replace"), end="")
    return result.returncode


def manifest_fixture(repo, root):
    """Fresh ordinary input package; no native/deployment artifacts copied."""
    package = pathlib.Path(tempfile.mkdtemp(prefix="manifest-package-", dir=root))
    shutil.copytree(repo / "crates/samples/stateless-test-app/manifests",
                    package, dirs_exist_ok=True)
    code = package / "EchoPackage/Executables"
    code.mkdir()
    shutil.copy2(repo / "target/debug/stateless-test-app", code / "native-echo")
    return package


def main():
    repo = pathlib.Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", *SCENARIOS])
    parser.add_argument("--root", type=pathlib.Path,
                        default=repo / "build/native-runtime-test-host")
    parser.add_argument("--native-directory", type=pathlib.Path)
    parser.add_argument("--dependency-directory", type=pathlib.Path)
    parser.add_argument("--diagnostics-directory", type=pathlib.Path,
                        help="retain <=1 MiB host/native console tail and raw report per scenario")
    args = parser.parse_args()
    root = args.root.resolve()
    if args.action == "prepare":
        native = prepare(root)
        qualify_libraries(native)
        print(f"Pinned native libraries: {native}")
        print("Supply matching OS ABI dependencies; no node service was installed.")
        return 0
    native = (args.native_directory or root / "runtime" / RELATIVE_NATIVE).resolve()
    dependencies = (args.dependency_directory or root / "deps").resolve()
    root.mkdir(parents=True, exist_ok=True)
    dependencies.mkdir(parents=True, exist_ok=True)
    qualify_libraries(native)
    environment = {"PATH": "/usr/bin:/bin",
                   "LD_LIBRARY_PATH": f"{native}:{dependencies}"}
    checked = subprocess.run(["ldd", str(native / "libFabricRuntime.so")],
                             env=environment, capture_output=True, text=True,
                             timeout=20, check=True)
    missing = [line.strip() for line in checked.stdout.splitlines() if "not found" in line]
    if missing:
        raise ValueError(f"missing native ABI prerequisites: {', '.join(missing)}")
    subprocess.run(["cargo", "build", "--locked", "-p", "mssf-test-host",
                    "-p", "stateless-test-app"], cwd=repo, check=True, timeout=180)
    if args.action.startswith("manifest-"):
        subprocess.run(["cargo", "build", "--locked", "-p", "mssf-test-host",
                        "--example", "manifest_acceptance"],
                       cwd=repo, check=True, timeout=180)
        package = manifest_fixture(repo, root)
        return run_host([
            str(repo / "target/debug/examples/manifest_acceptance"),
            str(native), str(dependencies), str(package), str(root),
            args.action.removeprefix("manifest-"),
        ], repo, args.diagnostics_directory, args.action)
    return run_host([
        str(repo / "target/debug/mssf-test-host"), str(native), str(dependencies),
        str(repo / "target/debug/stateless-test-app"), str(root), args.action,
    ], repo, args.diagnostics_directory, args.action)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"native profile failed: {error}", file=sys.stderr)
        sys.exit(1)
