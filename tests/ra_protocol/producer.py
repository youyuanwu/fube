"""Explicit producer: compile pinned native subset; package prebuilt Rust input.

Requires Clang with MS extensions, the authorized pinned source checkout and an
explicitly prebuilt Rust example. Never called by acceptance as a fallback.
"""
import argparse
import hashlib
import json
import platform
import shutil
import subprocess
from pathlib import Path

from .schema import MODELS, REVISION, generate

ROOT = Path(__file__).resolve().parents[2]
NATIVE = ROOT / "tests/ra_protocol/native"
FLAGS = ["--driver-mode=g++", "-std=c++17", "-fms-extensions", "-fdelayed-template-parsing",
         "-fshort-wchar", "-DPLATFORM_UNIX", "-Wno-everything", "-O1"]
SOURCES = ["FabricStream.cpp", "FabricSerializable.cpp",
           "FabricSerializableStream.cpp", "FabricSerializationTypes.cpp"]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_inputs():
    return sorted(
        [ROOT / "Cargo.toml", ROOT / "Cargo.lock"]
        + [p for crate in ["fube-ra", "fube-ra-store", "fube-transport"]
           for p in (ROOT / "crates/libs" / crate).rglob("*.rs")]
        + [ROOT / "crates/libs" / crate / "Cargo.toml"
           for crate in ["fube-ra", "fube-ra-store", "fube-transport"]]
        + list((ROOT / "tests/ra_protocol").glob("*.py"))
        + list(NATIVE.glob("*"))
        + list((ROOT / "tests/ra_protocol/helpers").glob("*"))
    )


def source_digest():
    pairs = {str(p.relative_to(ROOT)): digest(p) for p in source_inputs() if p.is_file()}
    return hashlib.sha256(json.dumps(pairs, sort_keys=True).encode()).hexdigest(), pairs


def produce(native, out, cxx, rust):
    revision = subprocess.check_output(
        ["git", "-C", str(native), "rev-parse", "HEAD"], text=True
    ).strip()
    if revision != REVISION:
        raise ValueError(f"wrong native source: {revision}")
    if subprocess.check_output(["git", "-C", str(native), "status", "--porcelain"], text=True):
        raise ValueError("native source must be unmodified")
    if platform.machine() != "x86_64" or platform.system() != "Linux":
        raise ValueError("source-baseline producer supports Linux x86-64 only")
    if not rust.is_file():
        raise ValueError("explicit prebuilt ra_conformance Rust executable required")
    source_hash, inputs = source_digest()
    if subprocess.check_output([str(rust), "--identity"], text=True).strip() != source_hash:
        raise ValueError("Rust executable not built with current FUBE_RA_SOURCE_DIGEST")
    out.mkdir(parents=True, exist_ok=False)
    (out / "native").mkdir()
    models = generate(native, out / "schema.h")
    prod = native / "src/prod"
    serial = prod / "shared/serialization"
    includes = [NATIVE, out, serial / "inc", prod / "src/Common", prod / "ktl/src/linuxinc"]
    command = [str(cxx), *FLAGS, f'-DFUBE_RA_SOURCE_DIGEST="{source_hash}"']
    for include in includes:
        command.extend(["-I", str(include)])
    command.extend(str(NATIVE / name) for name in ["platform.cpp", "fixture.cpp"])
    command.extend(str(serial / "src" / name) for name in SOURCES)
    command.extend(["-o", str(out / "native-oracle")])
    subprocess.run(command, check=True, timeout=180)
    shutil.copy2(rust, out / "ra-conformance")
    subprocess.run([str(out / "native-oracle"), "produce", str(out / "native")],
                   check=True, timeout=30)
    if source_digest()[0] != source_hash:
        raise ValueError("repository source changed during production")
    # Hash every native source/header used by the compilation, not merely the
    # revision label. Input reference stays read-only and unmodified.
    native_paths = set(serial.glob("inc/*.h")) | set(serial.glob("src/*.h"))
    native_paths |= {serial / "src" / name for name in SOURCES}
    native_paths |= {prod / "src/Common/FabricSerializationHelper.h",
                     prod / "ktl/src/linuxinc/kstatus.h"}
    native_paths |= {prod / "src" / row[1] for row in MODELS}
    native_hashes = {str(p.relative_to(native)): digest(p) for p in sorted(native_paths)}
    manifest = {
        "format": 1, "evidence": "source-baseline-native-stream-with-schema-adapters",
        "native_revision": revision, "target": "Linux-x86_64-short-wchar",
        "head": subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"],
                                        text=True).strip(),
        "source_digest": source_hash, "source_inputs": inputs,
        "native_sources": native_hashes, "compiler_flags": FLAGS,
        "compiler_version": subprocess.check_output([str(cxx), "--version"], text=True),
        "schema_types": len(models), "schema_fields": sum(len(m[4]) for m in models),
        "cases": sorted(p.stem for p in (out / "native").glob("*.bin")),
        "files": {str(p.relative_to(out)): digest(p) for p in sorted(out.rglob("*"))
                  if p.is_file()},
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps({key: manifest[key] for key in ["head", "source_digest", "cases"]}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--cxx", type=Path, required=True)
    parser.add_argument("--rust", type=Path, required=True)
    args = parser.parse_args()
    produce(args.native.resolve(), args.out.resolve(), args.cxx.resolve(), args.rust.resolve())


if __name__ == "__main__":
    main()
