#!/usr/bin/env python3
"""Test-only, placement-based engine selector; never selects a replica role."""
import hashlib
import json
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


def record_startup(work, name, owner, mode, changed):
    # Linux native console redirection does not reliably archive failed startup.
    # Preserve the first actual activated caller and its child's stderr per app.
    try:
        if len(list(work.glob("RustRuntimeService.FubeDataStartup.*.stderr"))) >= 16:
            return
        log = os.open(work / f"RustRuntimeService.FubeDataStartup.{os.getpid()}.stderr",
                      os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except OSError:
        return
    try:
        context = {"event": "FUBE_SELECTOR_CONTEXT", "pid": os.getpid(),
                   "uid": os.getuid(), "euid": os.geteuid(), "gid": os.getgid(),
                   "egid": os.getegid(), "groups": os.getgroups(),
                   "engine": name, "file_owner": owner, "mode": mode,
                   "mode_changed": changed}
        os.write(log, (json.dumps(context) + "\n").encode())
        os.dup2(log, 1)
        os.dup2(log, 2)
    finally:
        os.close(log)


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
        file = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        try:
            before = os.fstat(file)
            if (not stat.S_ISREG(before.st_mode)
                    or before.st_nlink != 1
                    or before.st_mode & (stat.S_ISUID | stat.S_ISGID | stat.S_ISVTX)
                    or before.st_size > 512 * 1024 * 1024):
                raise SystemExit("selected engine is not a regular packaged executable")
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
            mode = stat.S_IMODE(before.st_mode)
            executable = "/proc/self/fd/" + str(file)
            if not os.access(executable, os.X_OK, effective_ids=True):
                if before.st_uid != os.geteuid():
                    raise SystemExit("selected engine is not executable and is foreign-owned")
                # Only installation changes require ownership. Normal read/exec
                # honors the real activated caller's existing group/ACL access.
                mode |= stat.S_IXUSR
                os.fchmod(file, mode)
            installed = os.fstat(file)
            if stat.S_IMODE(installed.st_mode) != mode or not os.access(
                    executable, os.X_OK, effective_ids=True):
                raise SystemExit("selected engine owner-execute installation failed")
            record_startup(work, name, before.st_uid, mode,
                           mode != stat.S_IMODE(before.st_mode))
            # Execute the verified inode, not a path that could have been replaced.
            os.execve(file, [str(package / name)], dict(os.environ))
        finally:
            os.close(file)
    finally:
        os.close(directory)


if __name__ == "__main__":
    main()
