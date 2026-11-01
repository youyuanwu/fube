"""Direct operational Channel qualification on a fresh owned native onebox.

No caller-owned reuse, deployment, shared-service mutation or safeguard bypass.
The cluster is always destroyed with its original capability, even on partial
startup or child failure. Successful test output alone cannot prove cleanup.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import zlib

from tests import ROOT
from tests.onebox.shared import runtime

SOURCES = (
    "crates/libs/fube-transport/src/native_channel.rs",
    "crates/libs/fube-client/src/native_channel_live_tests.rs",
    "crates/libs/fube-client/src/notification_protocol.rs",
)


def reap_owned_child(child: subprocess.Popen) -> None:
    """Stop/reap this exact session before allocation cleanup can proceed."""
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        child.communicate(timeout=35)
    except BaseException:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.communicate(timeout=10)


def run_test(config: Path) -> dict:
    environment = dict(os.environ, FUBE_CHANNEL_NATIVE_CONFIG=str(config))
    child = subprocess.Popen(
        ["cargo", "test", "--locked", "-p", "fube-client", "native_channel_live",
         "--", "--ignored", "--nocapture"],
        cwd=ROOT, env=environment, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, start_new_session=True,
    )
    try:
        output, _ = child.communicate(timeout=300)
    except BaseException as primary:
        try:
            reap_owned_child(child)
        except BaseException as cleanup:
            primary.add_note(f"task-owned child cleanup failed: {cleanup}")
        if isinstance(primary, subprocess.TimeoutExpired):
            raise RuntimeError("bounded native Channel test timed out") from primary
        raise
    print(output, end="")
    if child.returncode:
        raise RuntimeError(f"direct native Channel test failed ({child.returncode})")
    records = re.findall(r"^CHANNEL_NATIVE_QUALIFIED (.+)$", output, flags=re.MULTILINE)
    if len(records) != 1 or "test result: ok. 1 passed; 0 failed; 0 ignored;" not in output:
        raise RuntimeError("missing actual direct native Channel exchange inventory")
    result = json.loads(records[0])
    for key in ("ordinary_rpc", "raw_full_local_write", "unchanged_native_notification",
                "original_reply_forwarded_to_sf"):
        if result.get(key) is not True:
            raise RuntimeError(f"native Channel capability was not qualified: {key}")
    supplied = json.loads(config.read_text())
    for key in ("runtime", "source_head", "source_sha256", "source_crc32"):
        if result.get(key) != supplied[key]:
            raise RuntimeError("direct native result source/runtime mismatch")
    return result


def qualify(name: str) -> dict:
    owner = None
    result = None

    def allocated(value: str) -> None:
        nonlocal owner
        owner = value

    try:
        runtime.create_owned(name, on_allocated=allocated)
        if owner is None:
            raise RuntimeError("native creation provided no original owner")
        allocation = runtime.Allocation(name, owner)
        allocation.verify()
        state = json.loads((ROOT / "build/oneboxes" / name / "state.json").read_text())
        source = b"".join((ROOT / path).read_bytes() for path in SOURCES)
        head = runtime.run("git", "rev-parse", "HEAD").strip()
        with tempfile.TemporaryDirectory(prefix="channel-native-", dir=ROOT / "build") as directory:
            os.chmod(directory, 0o700)
            config = Path(directory) / "config.json"
            payload = dict(allocation=name, owner=owner, container_id=allocation.container_id,
                           runtime=state["runtime_version"], endpoint="127.0.0.1:19000",
                           source_head=head, source_sha256=hashlib.sha256(source).hexdigest(),
                           source_crc32=zlib.crc32(source))
            with config.open("x", encoding="utf-8") as file:
                os.chmod(config, 0o600)
                json.dump(payload, file)
            result = run_test(config)
    finally:
        if owner is not None:
            cleanup = runtime.destroy_owned(name, owner)
            if cleanup.get("absence_verified") is not True:
                raise RuntimeError("native original resource absence not verified")
            print("Native Channel original container/volume/network absence verified.")
    if result is None:
        raise RuntimeError("native Channel qualification returned no result")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True, help="fresh task-owned onebox name")
    args = parser.parse_args()
    qualify(args.name)


if __name__ == "__main__":
    main()
