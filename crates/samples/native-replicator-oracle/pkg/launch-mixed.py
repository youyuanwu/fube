#!/usr/bin/env python3
"""Test-only, placement-based engine selector; never selects a replica role."""
import os
from pathlib import Path

NODE_ENGINES = {
    "N0010": "native-replicator-oracle",
    "N0020": "rust-runtime-app",
    "N0030": "native-replicator-oracle",
    "N0040": "rust-runtime-app",
    "N0050": "rust-runtime-app",
}


def main():
    node = os.environ.get("Fabric_NodeName")
    if node not in NODE_ENGINES:
        raise SystemExit("missing/unknown activated Fabric_NodeName; refusing engine selection")
    program = Path(__file__).resolve().parent / NODE_ENGINES[node]
    if program.is_symlink() or not program.is_file() or not os.access(program, os.X_OK):
        raise SystemExit("selected prebuilt engine is not a regular executable")
    os.execve(str(program), [str(program)], dict(os.environ))


if __name__ == "__main__":
    main()
