#!/usr/bin/env python3
"""Test-only exec launcher preserving the complete SF activation environment."""
import os
from pathlib import Path


def main():
    program = Path(__file__).resolve().parent / "rust-runtime-app"
    if program.is_symlink() or not program.is_file() or not os.access(program, os.X_OK):
        raise SystemExit("prebuilt Rust engine is not a regular executable")
    os.execve(str(program), [str(program)], dict(os.environ))


if __name__ == "__main__":
    main()
