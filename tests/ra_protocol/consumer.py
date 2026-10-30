"""Compiler-free acceptance. Requires independently expected commit/source hash.

No producer, Cargo, SF install, compiler search or fallback is imported/called.
"""
import argparse
import hashlib
import json
import subprocess
from pathlib import Path

REVISION = "3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d"
CASES = {
    "default-request", "rich-request", "default-reply", "rich-reply-0",
    "rich-reply--2147017398", "rich-reply--2147017441", "default-generation",
    "fm-generation", "fmm-generation", "actor-10", "actor-11", "actor-12",
    "action-request", "action-reply", "priority",
    "default-remove", "rich-remove-0", "rich-remove-1",
    "default-remove-reply", "rich-remove-reply", "explicit-empty-replica-up",
    "rich-replica-up-0-0", "rich-replica-up-0-1", "rich-replica-up-1-0", "rich-replica-up-1-1",
    "action-RemoveInstance", "action-RemoveInstanceReply", "action-ReplicaUp", "action-ReplicaUpReply",
}


def validate(bundle, expected_head, expected_source):
    if bundle.is_symlink() or not bundle.is_dir():
        raise ValueError("regular prebuilt bundle directory required")
    receipt = bundle / "manifest.json"
    if receipt.is_symlink() or not receipt.is_file() or receipt.stat().st_size > 1024 * 1024:
        raise ValueError("bounded regular manifest required")
    manifest = json.loads(receipt.read_text())
    if (manifest.get("format") != 1 or manifest.get("native_revision") != REVISION
            or manifest.get("head") != expected_head
            or manifest.get("source_digest") != expected_source
            or manifest.get("evidence") != "source-baseline-native-stream-with-schema-adapters"
            or manifest.get("target") != "Linux-x86_64-short-wchar"
            or set(manifest.get("cases", [])) != CASES):
        raise ValueError("prebuilt provenance/coverage mismatch")
    calculated_source = hashlib.sha256(
        json.dumps(manifest.get("source_inputs", {}), sort_keys=True).encode()
    ).hexdigest()
    if (calculated_source != expected_source or manifest.get("schema_types") != 39
            or manifest.get("schema_fields") != 123
            or not manifest.get("native_sources")):
        raise ValueError("source/schema manifest mismatch")
    expected_files = {"schema.h", "native-oracle", "ra-conformance"} | {
        f"native/{name}.bin" for name in CASES
    }
    if set(manifest.get("files", {})) != expected_files:
        raise ValueError("wrong prebuilt inventory")
    actual_files = {str(p.relative_to(bundle)) for p in bundle.rglob("*") if p.is_file()}
    if actual_files != expected_files | {"manifest.json"}:
        raise ValueError("missing/extra prebuilt artifacts")
    for name in expected_files:
        path = bundle / name
        if (path.is_symlink() or path.parent.is_symlink() or not path.is_file()
                or path.stat().st_size > 64 * 1024 * 1024
                or hashlib.sha256(path.read_bytes()).hexdigest() != manifest["files"][name]):
            raise ValueError(f"prebuilt integrity mismatch: {name}")
    return manifest


def consume(bundle, out, expected_head, expected_source):
    manifest = validate(bundle, expected_head, expected_source)
    for executable in ["ra-conformance", "native-oracle"]:
        identity = subprocess.check_output(
            [str(bundle / executable), "--identity"], text=True, timeout=10
        ).strip()
        if identity != expected_source:
            raise ValueError(f"{executable}: compiled source identity mismatch")
    out.mkdir(parents=True, exist_ok=False)
    # Executables use absolute verified paths and bounded execution. The caller
    # supplies a fresh output directory; acceptance never regenerates inputs.
    subprocess.run([str(bundle / "ra-conformance"), str(bundle / "native"), str(out)],
                   check=True, timeout=30)
    subprocess.run([str(bundle / "native-oracle"), "verify", str(out)],
                   check=True, timeout=30)
    print(f"PASS: {len(manifest['cases'])} bidirectional source-baseline native cases")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--expected-source", required=True)
    args = parser.parse_args()
    consume(args.bundle.absolute(), args.out.absolute(), args.expected_head, args.expected_source)


if __name__ == "__main__":
    main()
