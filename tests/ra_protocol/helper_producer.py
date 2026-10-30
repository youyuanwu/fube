"""Explicit actual-native helper producer, separate from the 36-case wire gate.

Compiles original PAL C and retry-policy TUs, plus verbatim identifier and
generation methods. --rust optionally compares an explicitly prebuilt,
source-bound ra_helper_conformance executable; never builds Rust implicitly.
"""
import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
from pathlib import Path

from .producer import ROOT, digest, source_digest
from .schema import REVISION

HELPERS = ROOT / "tests/ra_protocol/helpers"
CFLAGS = [
    "-x", "c", "-std=gnu11", "-fms-extensions", "-fshort-wchar", "-fno-builtin",
    "-DUSE_STDLIB=1", "-DPLATFORM_UNIX=1", "-DPAL_STDCPP_COMPAT=1",
    "-Wno-everything", "-Wno-implicit-function-declaration",
    "-include", "stdint.h", "-include", "wctype.h",
    "-ffunction-sections", "-fdata-sections",
]
CXXFLAGS = [
    "--driver-mode=g++", "-std=c++17", "-fms-extensions", "-fshort-wchar",
    "-fno-builtin", "-Wno-everything", "-O0", "-ffunction-sections",
    "-fdata-sections",
]
OUTPUTS = ["trace.txt", "upper-units.bin"]


def clean_revision(native):
    revision = subprocess.check_output(
        ["git", "-C", str(native), "rev-parse", "HEAD"], text=True
    ).strip()
    if revision != REVISION:
        raise ValueError(f"wrong native source: {revision}")
    if subprocess.check_output(["git", "-C", str(native), "status", "--porcelain"], text=True):
        raise ValueError("native source must be unmodified")
    return revision


def produce(native, out, cxx, rust=None):
    if out == native or native in out.parents:
        raise ValueError("helper output must not modify the native reference")
    revision = clean_revision(native)
    if (platform.system(), platform.machine()) != ("Linux", "x86_64"):
        raise ValueError("helper evidence supports Linux x86-64 only")
    source_hash, source_inputs = source_digest()
    if rust is not None:
        if not rust.is_file():
            raise ValueError("explicit prebuilt ra_helper_conformance executable required")
        if subprocess.check_output([str(rust), "--identity"], text=True).strip() != source_hash:
            raise ValueError("Rust executable not built with current FUBE_RA_SOURCE_DIGEST")
    out.mkdir(parents=True, exist_ok=False)
    for folder in ["scratch", "native", "rust"]:
        (out / folder).mkdir()
    # Native compiler scratch must never use the host's default temporary path.
    env = {**os.environ, "TMPDIR": str(out / "scratch")}
    commands = []
    methods = []
    inputs = set()
    src = native / "src/prod/src"
    ra = src / "Reliability/Failover/ra"

    def run(command, **kwargs):
        commands.append({"argv": [str(arg) for arg in command],
                         "cwd": str(kwargs.get("cwd", ROOT))})
        return subprocess.run(command, env=env, check=True, timeout=180, **kwargs)

    def extract(path, signature):
        text = path.read_text()
        start = text.index(signature)
        opening = text.index("{", start)
        depth, end = 1, opening + 1
        while depth:
            depth += (text[end] == "{") - (text[end] == "}")
            end += 1
        method = text[start:end]
        inputs.add(path)
        methods.append({"path": str(path.relative_to(native)),
                        "start": text.count("\n", 0, start) + 1,
                        "end": text.count("\n", 0, end) + 1,
                        "sha256": hashlib.sha256(method.encode()).hexdigest()})
        return method

    utility = [extract(src / "Common/StringUtility.h", signature) for signature in [
        "inline static bool AreEqualCaseInsensitive(std::wstring const & a, std::wstring const & b)",
        "inline static int CompareCaseInsensitive(std::wstring const & a, std::wstring const & b)",
    ]]
    (out / "GeneratedStringUtility.h").write_text(
        "// Verbatim methods; only the wstring storage token is adapted.\n"
        "#define wstring basic_string<char16_t>\nnamespace Common { struct StringUtility { public:\n"
        + "\n".join(utility) + "\n}; }\n#undef wstring\n")
    identifiers = []
    for name in ["ApplicationIdentifier", "ServicePackageIdentifier", "ServiceTypeIdentifier"]:
        path = src / f"ServiceModel/{name}.cpp"
        if name == "ApplicationIdentifier":
            identifiers.append(extract(path, "bool ApplicationIdentifier::IsEmpty() const"))
        for signature in [f"bool {name}::operator == ", f"bool {name}::operator != ",
                          f"int {name}::compare(", f"bool {name}::operator < "]:
            identifiers.append(extract(path, signature))
    (out / "GeneratedModelMethods.h").write_text(
        "using namespace Common;\nusing namespace ServiceModel;\n"
        + "\n\n".join(identifiers) + "\n")

    generation = []
    for name, path in [
        ("LargeInteger", src / "Common/LargeInteger.cpp"),
        ("NodeId", src / "ServiceModel/federation/NodeId.cpp"),
        ("GenerationNumber", src / "Reliability/Failover/GenerationNumber.cpp"),
    ]:
        constructors = {
            "LargeInteger": ["LargeInteger::LargeInteger()", "LargeInteger::LargeInteger(uint64 high",
                             "LargeInteger::LargeInteger(LargeInteger const & value)",
                             "LargeInteger::~LargeInteger()"],
            "NodeId": ["NodeId::NodeId(LargeInteger const& from)", "NodeId::NodeId()"],
            "GenerationNumber": ["GenerationNumber::GenerationNumber()",
                                 "GenerationNumber::GenerationNumber(int64 generation"],
        }[name]
        generation += [extract(path, signature) for signature in constructors]
        for op in ["<", ">", "<=", ">=", "==", "!="]:
            spacing = "" if name == "GenerationNumber" else " "
            generation.append(extract(path, f"bool {name}::operator{spacing}{op}{spacing}("))
    generation += [extract(ra / "GenerationStateManager.cpp", signature) for signature in [
        "bool GenerationStateManager::CheckGenerationHeader(",
        "GenerationState & GenerationStateManager::GetGenerationState(bool",
    ]]
    (out / "GeneratedGenerationMethods.h").write_text(
        "using namespace Common;\nusing namespace Federation;\nusing namespace Reliability;\n"
        "using namespace Reliability::ReconfigurationAgentComponent;\nusing std::wstring;\n"
        "Federation::NodeId const Federation::NodeId::MinNodeId(Common::LargeInteger(0, 0));\n"
        + "\n\n".join(generation) + "\n")

    config = src / "Reliability/Failover/common/FailoverConfig.h"
    inputs.add(config)
    defaults = re.findall(
        r"^\s*\w+_CONFIG_ENTRY\(int,\s*L\"ReconfigurationAgent\",\s*(\w+),\s*(-?\d+),",
        config.read_text(), re.M)
    (out / "GeneratedConfig.h").write_text(
        "// Native signed int defaults; mutable transport fields, no config loader.\n"
        "namespace Reliability { struct FailoverConfig {\n"
        + "".join(f"int {name} = {value};\n" for name, value in defaults) + "}; }\n")
    for name in ["RetryableErrorState.cpp", "RetryableErrorStateThresholdCollection.cpp"]:
        inputs.add(ra / name)
        shutil.copyfile(ra / name, out / name)
    headers = ["RetryableErrorStateName.h", "RetryableErrorAction.h",
               "RetryableErrorStateThresholdCollection.h", "RetryableErrorState.h"]
    inputs.update(ra / name for name in headers)
    (out / "Ra.Stdafx.h").write_text(
        '#include "adapter.h"\n' + "".join(f'#include "{ra / name}"\n' for name in headers))
    for name in ["adapter.h", "main.cpp"]:
        shutil.copyfile(HELPERS / name, out / name)

    pal = src / "pal/wc16.c"
    inputs.add(pal)
    cflags = CFLAGS + ["-I" + str(src / "pal/src"), "-I" + str(src / "pal/src/internal")]
    run([cxx, *cflags, "-MMD", "-MF", out / "wc16.d", "-c", pal, "-o", out / "wc16.o"])
    macros = run([cxx, *cflags, "-E", "-dM", pal], text=True, capture_output=True)
    (out / "pal-macros.txt").write_text("\n".join(
        line for line in macros.stdout.splitlines()
        if re.match(r"#define (towupper|_wcsicmp|USE_STDLIB|PAL_STDCPP_COMPAT|__WCHAR_MAX__)\b", line)
    ) + "\n")
    symbols = run(["nm", "-u", out / "wc16.o"], text=True, capture_output=True)
    (out / "undefined-symbols.txt").write_text(symbols.stdout)
    if not re.search(r"\bU towupper\b", symbols.stdout):
        raise ValueError("native PAL comparator must resolve libc towupper")
    for token in (out / "wc16.d").read_text().replace("\\\n", " ").split():
        if token.startswith(str(native) + "/"):
            inputs.add(Path(token))
    run([cxx, *CXXFLAGS, "-MMD", "-MF", out / "helpers.d",
         f'-DFUBE_RA_SOURCE_DIGEST="{source_hash}"', "-I" + str(out),
         out / "main.cpp", out / "RetryableErrorState.cpp",
         out / "RetryableErrorStateThresholdCollection.cpp", out / "wc16.o",
         "-Wl,--gc-sections", "-o", out / "native-helpers"])
    if run([out / "native-helpers", "--identity"], text=True,
           capture_output=True).stdout.strip() != source_hash:
        raise ValueError("native source identity mismatch")
    result = run([out / "native-helpers"], cwd=out / "native", text=True, capture_output=True)
    (out / "native/trace.txt").write_text(result.stdout)
    comparisons = {}
    if rust is not None:
        shutil.copy2(rust, out / "ra-helper-conformance")
        result = run([out / "ra-helper-conformance"], cwd=out / "rust",
                     text=True, capture_output=True)
        (out / "rust/trace.txt").write_text(result.stdout)
        comparisons = {name: digest(out / "native" / name) == digest(out / "rust" / name)
                       for name in OUTPUTS}
    if source_digest()[0] != source_hash:
        raise ValueError("repository source changed during helper production")
    clean_revision(native)
    manifest = {
        "format": 1,
        "evidence": "original-PAL-C-and-retry-policy-TUs-plus-verbatim-identifier-generation-methods",
        "native_revision": revision,
        "head": subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"],
                                        text=True).strip(),
        "source_digest": source_hash, "source_inputs": source_inputs,
        "native_sources": {str(p.relative_to(native)): digest(p) for p in sorted(inputs)},
        "extracted_methods": methods, "commands": commands,
        "compiler_version": subprocess.check_output([str(cxx), "--version"], text=True),
        "environment": {key: env.get(key) for key in ["TMPDIR", "LD_LIBRARY_PATH", "LANG", "LC_ALL"]},
        "adapters": [
            "UTF-16 container/storage bridge to original PAL C comparator; startup C locale",
            "identifier/GenerationNumber/NodeId/LargeInteger type shells; verbatim native methods",
            "signed mutable FailoverConfig fields with extracted native defaults, no loader",
            "CheckGenerationHeader/GetGenerationState verbatim; lock and tracing are no-ops",
            "native failure action raw log retained; Restart/Error mapped to None as Rust handlers do",
            "mixed read uses native reader callbacks and Rust production RetryPolicy::read",
        ],
        "comparisons": comparisons,
        "files": {str(p.relative_to(out)): digest(p) for p in sorted(out.rglob("*")) if p.is_file()},
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    if comparisons and not all(comparisons.values()):
        raise ValueError(f"actual-native/Rust helper mismatch: {comparisons}; see {out}")
    print(json.dumps({key: manifest[key] for key in ["head", "source_digest", "comparisons"]},
                     indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ["native", "out", "cxx"]:
        parser.add_argument("--" + flag, type=Path, required=True)
    parser.add_argument("--rust", type=Path, help="explicit prebuilt ra_helper_conformance")
    args = parser.parse_args()
    produce(args.native.resolve(), args.out.resolve(), args.cxx.resolve(),
            args.rust.resolve() if args.rust is not None else None)


if __name__ == "__main__":
    main()
