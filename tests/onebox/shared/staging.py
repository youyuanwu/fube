"""Owned staging for trusted, immutable test packages; no hostile concurrent writers."""

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import sys

PARENT = Path("/var/lib/sf-node")
OWNER = ".fube-upload-owner"
MAX_ENTRIES = 64
MAX_BYTES = 512 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def inventory(source):
    source = Path(source)
    require(stat.S_ISDIR(source.lstat().st_mode), "package root must be a regular directory")
    result, total = {}, 0
    for path in source.rglob("*"):
        require(len(result) < MAX_ENTRIES, "package entry bound")
        name = path.relative_to(source).as_posix()
        parts = name.split("/")
        require(len(name) <= 98 and len(parts) <= 8
                and all(re.fullmatch(r"[A-Za-z0-9_.-]+", part) and part not in (".", "..", "_.dir")
                        for part in parts)
                and parts[0] != OWNER, "invalid package path")
        info = path.lstat()
        mode = stat.S_IMODE(info.st_mode)
        if stat.S_ISDIR(info.st_mode):
            result[name] = {"kind": "directory", "mode": mode}
            continue
        require(stat.S_ISREG(info.st_mode) and mode in (0o644, 0o755),
                "package accepts regular files with modes 0644/0755 only")
        total += info.st_size
        require(total <= MAX_BYTES, "package byte bound")
        digest, size = hashlib.sha256(), 0
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as file:
            opened = os.fstat(file.fileno())
            require((opened.st_dev, opened.st_ino, opened.st_size, opened.st_mode)
                    == (info.st_dev, info.st_ino, info.st_size, info.st_mode), "package file changed")
            for chunk in iter(lambda: file.read(65536), b""):
                size += len(chunk)
                require(size <= info.st_size, "package file grew")
                digest.update(chunk)
        require(size == info.st_size, "package file size changed")
        result[name] = {"kind": "file", "mode": mode, "bytes": size, "sha256": digest.hexdigest()}
    require(result, "empty package")
    return result


def reservation(parent, path, action, proof):
    parent, path = Path(parent), Path(path)
    require(parent.is_absolute() and path.parent == parent
            and re.fullmatch(r"fube-data-[a-f0-9]{32}", path.name), "invalid upload reservation path")
    require(action in ("reserve", "verify", "remove"), "invalid upload reservation action")
    if action == "reserve":
        require(type(proof) is dict and set(proof) == {"capability"}
                and re.fullmatch(r"[a-f0-9]{64}", proof["capability"]), "invalid upload capability")
    else:
        require(type(proof) is dict and set(proof) == {"parent", "directory", "owner_file", "capability"}
                and all(type(proof[key]) is list and len(proof[key]) == 2
                        and all(type(value) is int and value >= 0 for value in proof[key])
                        for key in ("parent", "directory", "owner_file"))
                and re.fullmatch(r"[a-f0-9]{64}", proof["capability"]), "invalid upload ownership")
    parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    directory_fd = None
    def identity(fd):
        info = os.fstat(fd)
        return [info.st_dev, info.st_ino]
    try:
        if action == "reserve":
            os.mkdir(path.name, 0o700, dir_fd=parent_fd)
        directory_fd = os.open(path.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL if action == "reserve" else os.O_RDONLY
        owner_fd = os.open(OWNER, flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=directory_fd)
        try:
            if action == "reserve":
                require(os.write(owner_fd, proof["capability"].encode("ascii")) == 64, "short upload owner write")
                proof = {**proof, "parent": identity(parent_fd), "directory": identity(directory_fd),
                         "owner_file": identity(owner_fd)}
            info = os.fstat(owner_fd)
            require(stat.S_ISREG(info.st_mode) and info.st_size == 64 and info.st_nlink == 1
                    and identity(owner_fd) == proof["owner_file"], "upload owner capability changed; retain")
            if action != "reserve":
                require(os.read(owner_fd, 65).decode("ascii") == proof["capability"],
                        "upload owner capability changed; retain")
            current, ancestor = path.lstat(), parent.lstat()
            require(stat.S_ISDIR(current.st_mode) and [current.st_dev, current.st_ino] == proof["directory"]
                    and identity(directory_fd) == proof["directory"]
                    and [ancestor.st_dev, ancestor.st_ino] == identity(parent_fd) == proof["parent"],
                    "upload directory incarnation changed; retain")
        finally:
            os.close(owner_fd)
        if action == "remove":
            shutil.rmtree(path)
            require(not os.path.lexists(path), "upload reservation removal unconfirmed")
        return proof
    finally:
        if directory_fd is not None:
            os.close(directory_fd)
        os.close(parent_fd)


def main():
    parent, path, action, encoded = sys.argv[1:]
    proof = json.loads(encoded)
    result = reservation(parent, path, "verify" if action == "inventory" else action, proof)
    if action == "inventory":
        require({entry.name for entry in Path(path).iterdir()} == {OWNER, Path(path).name},
                "upload staging inventory changed")
        result = inventory(Path(path) / Path(path).name)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
