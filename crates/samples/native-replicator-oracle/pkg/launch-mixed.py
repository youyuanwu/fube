#!/usr/bin/env python3
"""Test-only, placement-based engine selector; never selects a replica role."""
import hashlib
import os
from pathlib import Path
import re
import stat

# Filled upstream from the exact strict prebuilt binaries, before bundle hashing.
EXPECTED_SHA256 = {
    "native-replicator-oracle": "@native-replicator-oracle:sha256@",
    "rust-runtime-app": "@rust-runtime-app:sha256@",
}

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
    name = NODE_ENGINES[node]
    expected = EXPECTED_SHA256[name]
    if not re.fullmatch(r"[a-f0-9]{64}", expected):
        raise SystemExit("selected engine has no strict prebuilt SHA256")
    package = Path(__file__).absolute().parent
    work = Path(os.environ.get("Fabric_Folder_App_Work", ""))
    if (package.resolve() != package or Path.cwd() != package
            or not work.is_absolute() or work.parent != package.parent
            or os.environ.get("Fabric_CodePackageName") != "Code"
            or not os.environ.get("Fabric_ApplicationHostId")):
        raise SystemExit("selector is outside its activation-owned code package")
    directory = os.open(package, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        if os.fstat(directory).st_uid != os.getuid():
            raise SystemExit("foreign code package owner")
        file = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        try:
            before = os.fstat(file)
            if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                    or before.st_nlink != 1
                    or before.st_mode & (stat.S_ISUID | stat.S_ISGID | stat.S_ISVTX)
                    or before.st_size > 512 * 1024 * 1024):
                raise SystemExit("selected engine is not an owned regular packaged executable")
            digest = hashlib.sha256()
            size = 0
            while chunk := os.read(file, 1024 * 1024):
                size += len(chunk)
                if size > before.st_size:
                    raise SystemExit("selected engine changed while hashing")
                digest.update(chunk)
            after = os.fstat(file)
            identity = ("st_dev", "st_ino", "st_uid", "st_gid", "st_mode", "st_size",
                        "st_nlink", "st_mtime_ns", "st_ctime_ns")
            if digest.hexdigest() != expected or any(
                    getattr(after, field) != getattr(before, field) for field in identity):
                raise SystemExit("selected engine strict prebuilt hash/identity mismatch")
            if not before.st_mode & stat.S_IXUSR:
                # SF grants execute to the declared selector, not child payloads.
                # Install only the missing owner-execute bit on this verified FD.
                os.fchmod(file, stat.S_IMODE(before.st_mode) | stat.S_IXUSR)
            installed = os.fstat(file)
            if stat.S_IMODE(installed.st_mode) != stat.S_IMODE(before.st_mode) | stat.S_IXUSR:
                raise SystemExit("selected engine owner-execute installation failed")
            # Execute the verified inode, not a path that could have been replaced.
            os.execve(file, [str(package / name)], dict(os.environ))
        finally:
            os.close(file)
    finally:
        os.close(directory)


if __name__ == "__main__":
    main()
