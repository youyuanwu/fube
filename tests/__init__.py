from pathlib import Path
import sys

if sys.platform != "linux":
    raise RuntimeError("fube test infrastructure supports Linux only")

ROOT = Path(__file__).resolve().parents[1]
