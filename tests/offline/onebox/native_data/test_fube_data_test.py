"""Offline harness-contract tests. These do not claim native qualification."""
import contextlib
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
import uuid
from unittest.mock import MagicMock, patch
import xml.etree.ElementTree as ET

from tests.onebox.shared import data


GRAPHS = {
    "rust-runtime-app": "rust-runtime-app\n fube-data\n fube-runtime",
    "runtime_observer": "fube-client\n fube-transport",
    "native-replicator-oracle": "native-replicator-oracle\n mssf-core v0.10.0\n mssf-com v0.10.0\n fube-data",
}
DYNAMIC = "\nDynamic section:\n (NEEDED) Shared library: [libc.so.6]\n"
LEGACY_PKG = data.ROOT / "crates/samples/rust-runtime-app/pkg"


def executable(name):
    header = bytearray(64)
    header[:6] = b"\x7fELF\x02\x01"
    struct.pack_into("<H", header, 18, 62)
    return bytes(header) + name.encode()


def store_generation(expected, generation=(131510254310497004, 23, 25769803776)):
    return {"StoreFiles": [{"StoreRelativePath": data.type_store_path(expected),
                           "FileVersion": dict(zip(data.TYPE_IDENTITY_METHOD["native_generation_fields"],
                                                   map(str, generation)))}], "StoreFolders": []}


class ProjectFiles(unittest.TestCase):
    def setUp(self):
        self.root = data.ROOT / "build/fube-data-unit" / uuid.uuid4().hex
        self.root.mkdir(parents=True, exist_ok=False)
        self.addCleanup(shutil.rmtree, self.root)
        reader = patch.object(data.runtime, "run", side_effect=self.readelf_only)
        reader.start()
        self.addCleanup(reader.stop)

    @staticmethod
    def readelf_only(*args, **kwargs):
        if args[:2] == ("readelf", "-d"):
            return DYNAMIC
        raise AssertionError(f"qualification attempted an unexpected command: {args}")

    def make_legacy(self, directory):
        directory.mkdir(parents=True)
        hashes = {}
        for name in ("rust-runtime-app", "runtime_observer"):
            path = directory / name
            path.write_bytes(executable(name))
            path.chmod(0o755)
            hashes[name] = data.runtime.digest(path)
        for version in (1, 2):
            for name in data.runtime.PACKAGE_FILES:
                path = directory / f"package-v{version}" / name
                path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(LEGACY_PKG / name, path)
                path.chmod(0o644)
            if version == 2:
                data.runtime.upgrade_template(directory / "package-v2")
            for name in data.runtime.PACKAGE_FILES:
                path = directory / f"package-v{version}" / name
                hashes[f"package-v{version}/{name}"] = data.runtime.digest(path)
        manifest = directory / "build-info.json"
        manifest.write_text(json.dumps({
            "version": data.runtime.BUNDLE_VERSION, "profile": data.runtime.BUNDLE_PROFILE,
            "source_revision": "a" * 40, "dependency_graph": GRAPHS["rust-runtime-app"], "sha256": hashes,
        }))
        manifest.chmod(0o644)

    def bundle(self, name="bundle"):
        directory = self.root / name
        directory.mkdir()
        self.make_legacy(directory / "legacy")
        for name, mode in data.expected_files().items():
            path = directory / name
            if name.startswith("legacy/"):
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            if name.startswith("bin/"):
                path.write_bytes(executable(path.name))
            elif name.startswith("launchers/"):
                if path.name == "launch-mixed.py":
                    path.write_text(data.render_mixed_launcher(directory))
                else:
                    shutil.copyfile(data.PKG / path.name, path)
            else:
                _, profile, relative = name.split("/", 2)
                shutil.copyfile(data.PKG / profile / relative, path)
            path.chmod(mode)
        metadata = {
            "version": data.BUNDLE_VERSION, "profile": data.BUNDLE_PROFILE, "source_revision": "a" * 40,
            "dependency_graphs": copy.deepcopy(GRAPHS),
            "elf": {name: data.elf_identity(directory / "bin" / name, data.CLASSIFICATIONS[name])
                    for name in data.ENGINES},
            "artifacts": {name: {"sha256": data.runtime.digest(directory / name), "mode": mode}
                          for name, mode in data.expected_files().items()},
        }
        manifest = directory / "build-info.json"
        manifest.write_text(json.dumps(metadata))
        manifest.chmod(0o644)
        return directory

    @staticmethod
    def mutate(directory, action):
        path = directory / "build-info.json"
        metadata = json.loads(path.read_text())
        action(metadata)
        path.write_text(json.dumps(metadata))

    def cli(self, directory, *, create=None, owner=None, name="owned", report="evidence.json"):
        args = ["fube_data_test.py", name, "--prebuilt", str(directory), "--profile", "rust",
                "--report", str(self.root / report)]
        if owner is not None:
            args += ["--existing-owner", owner]
        with patch("sys.argv", args), contextlib.redirect_stdout(io.StringIO()) as output:
            with self.assertRaises(SystemExit) as exit:
                data.main()
            self.assertEqual(exit.exception.code, 1)
            return json.loads(output.getvalue())


class BundleTests(ProjectFiles):
    def test_roundtrip_exact_artifacts_source_and_native_classification_without_cargo(self):
        directory = self.bundle()
        bundle = data.load_bundle(directory)
        self.assertEqual(bundle.source_revision, "a" * 40)
        self.assertEqual(bundle.observer, directory / "bin/runtime_observer")
        self.assertEqual(bundle.legacy.app.read_bytes(), bundle.binary("rust").read_bytes())
        self.assertEqual(bundle.metadata["elf"]["native-replicator-oracle"]["classification"], "native-classic-dynamic")
        self.assertEqual(set(bundle.metadata["artifacts"]), set(data.expected_files()))

    def test_metadata_exact_schema_profile_version_and_source(self):
        cases = [
            ("version", 0), ("version", True), ("profile", "empty-control"),
            ("source_revision", "dirty"), ("source_revision", "A" * 40), ("extra", True),
            ("dependency_graphs", {}), ("elf", {}), ("artifacts", {}),
        ]
        for index, (field, value) in enumerate(cases):
            with self.subTest(field=field, value=value):
                directory = self.bundle(f"bad-{index}")
                self.mutate(directory, lambda metadata: metadata.update({field: value}))
                with self.assertRaises(RuntimeError):
                    data.load_bundle(directory)

    def test_duplicate_json_keys_are_rejected(self):
        directory = self.bundle()
        path = directory / "build-info.json"
        text = path.read_text()
        path.write_text(text[:-1] + ',"version":1}')
        with self.assertRaisesRegex(RuntimeError, "duplicate JSON"):
            data.load_bundle(directory)

    def test_inventory_stops_at_first_excess_entry_without_materializing_the_walk(self):
        directory = self.bundle()
        visited = []
        def inventory(_path, _pattern):
            for index in range(100000):
                visited.append(index)
                if index > 160:
                    self.fail("continued walking after the first over-bound entry")
                yield directory / str(index)
        with patch.object(Path, "rglob", inventory):
            with self.assertRaisesRegex(RuntimeError, "inventory bound"):
                data.load_bundle(directory)
        self.assertEqual(len(visited), 161)

    def test_self_consistent_old_or_prepopulated_ownership_templates_fail_before_allocation(self):
        for index, replacement in enumerate(("", "copied-owner-value")):
            directory = self.bundle(f"ownership-template-{index}")
            path = directory / "packages/rust/ApplicationManifest.xml"
            original = '<Parameter Name="FubeDataCreationCapability" DefaultValue="" />'
            path.write_text(path.read_text().replace(original, "" if not replacement else
                '<Parameter Name="FubeDataCreationCapability" DefaultValue="copied-owner-value" />'))
            self.mutate(directory, lambda metadata: metadata["artifacts"][
                "packages/rust/ApplicationManifest.xml"].update(sha256=data.runtime.digest(path)))
            with patch.object(data.runtime, "create_owned") as create, \
                 patch.object(data.runtime, "Allocation") as allocation:
                result = self.cli(directory, report=f"ownership-template-{index}.json")
            create.assert_not_called()
            allocation.assert_not_called()
            self.assertIn("ownership parameter templates before allocation", result["errors"][0])

    def test_dependency_graphs_never_relax_rust_or_observer_native_boundary(self):
        for index, (name, graph) in enumerate([
            ("rust-runtime-app", GRAPHS["rust-runtime-app"] + "\n mssf-core"),
            ("runtime_observer", "fube-client\n mssf-com"),
            ("runtime_observer", "fube-client\n FabricClient"),
            ("native-replicator-oracle", "native-replicator-oracle\n fube-data"),
            ("rust-runtime-app", ""),
        ]):
            directory = self.bundle(str(index))
            self.mutate(directory, lambda metadata: metadata["dependency_graphs"].update({name: graph}))
            with self.assertRaises(RuntimeError):
                data.load_bundle(directory)

    def test_prepopulated_legacy_owners_fail_before_allocation_even_with_consistent_nested_hashes(self):
        for number, parameter in ((1, "FubeDataTypeCapability"), (2, "FubeDataCreationCapability")):
            with self.subTest(version=number, parameter=parameter):
                directory = self.bundle(f"legacy-owner-{number}")
                relative = f"package-v{number}/ApplicationManifest.xml"
                path = directory / "legacy" / relative
                manifest = ET.parse(path)
                parameters = ET.Element(f"{{{data.runtime.NS}}}Parameters")
                ET.SubElement(parameters, f"{{{data.runtime.NS}}}Parameter", Name=parameter,
                              DefaultValue="a" * 64)
                manifest.getroot().insert(0, parameters)
                manifest.write(path, encoding="utf-8", xml_declaration=True)
                metadata_path = directory / "legacy/build-info.json"
                metadata = json.loads(metadata_path.read_text())
                metadata["sha256"][relative] = data.runtime.digest(path)
                metadata_path.write_text(json.dumps(metadata))
                self.mutate(directory, lambda value: value["artifacts"]["legacy/" + relative].update(
                    sha256=data.runtime.digest(path)))
                self.mutate(directory, lambda value: value["artifacts"]["legacy/build-info.json"].update(
                    sha256=data.runtime.digest(metadata_path)))
                with patch.object(data.runtime, "create_owned") as create, \
                     patch.object(data.runtime, "Allocation") as allocation:
                    result = self.cli(directory, report=f"legacy-owner-{number}.json")
                create.assert_not_called()
                allocation.assert_not_called()
                self.assertIn("legacy bundle ownership parameters must be absent or empty", result["errors"][0])

    def test_extra_missing_symlink_fifo_tamper_and_modes_are_rejected(self):
        for case in ("extra-file", "extra-dir", "missing", "symlink", "fifo", "tamper",
                     "not-executable", "world-writable", "setuid", "metadata-mode", "metadata-fifo"):
            with self.subTest(case=case):
                directory = self.bundle(case)
                path = directory / "bin/native-replicator-oracle"
                if case == "extra-file":
                    (directory / "unexpected").write_bytes(b"extra")
                elif case == "extra-dir":
                    (directory / "unexpected").mkdir()
                elif case == "missing":
                    path.unlink()
                elif case == "symlink":
                    path.unlink()
                    path.symlink_to(directory / "bin/rust-runtime-app")
                elif case == "fifo":
                    os.mkfifo(directory / "unexpected")
                elif case == "tamper":
                    path.write_bytes(path.read_bytes() + b"tampered")
                elif case == "not-executable":
                    path.chmod(0o644)
                elif case == "world-writable":
                    path.chmod(0o777)
                elif case == "setuid":
                    path.chmod(0o4755)
                elif case == "metadata-mode":
                    (directory / "build-info.json").chmod(0o666)
                else:
                    manifest = directory / "build-info.json"
                    manifest.unlink()
                    os.mkfifo(manifest)
                with self.assertRaises(RuntimeError):
                    data.load_bundle(directory)

    def test_artifact_records_and_elf_evidence_have_exact_keys(self):
        for index, action in enumerate([
            lambda metadata: metadata["artifacts"]["bin/rust-runtime-app"].update(extra=1),
            lambda metadata: metadata["artifacts"]["bin/rust-runtime-app"].update(sha256="0" * 64),
            lambda metadata: metadata["artifacts"]["bin/rust-runtime-app"].update(mode=0o777),
            lambda metadata: metadata["elf"]["native-replicator-oracle"].update(classification="native-free"),
            lambda metadata: metadata["elf"]["rust-runtime-app"].update(dynamic="invented"),
            lambda metadata: metadata["elf"]["runtime_observer"].update(extra=True),
        ]):
            directory = self.bundle(str(index))
            self.mutate(directory, action)
            with self.assertRaises(RuntimeError):
                data.load_bundle(directory)

    def test_changed_elf_header_is_rejected_even_if_its_hash_was_updated(self):
        directory = self.bundle()
        path = directory / "bin/rust-runtime-app"
        content = bytearray(path.read_bytes())
        struct.pack_into("<H", content, 18, 183)
        path.write_bytes(content)
        self.mutate(directory, lambda metadata: metadata["artifacts"]["bin/rust-runtime-app"].update(
            sha256=data.runtime.digest(path)))
        with self.assertRaisesRegex(RuntimeError, "x86-64 ELF"):
            data.load_bundle(directory)

    def test_native_dynamic_import_is_rejected_for_production_only(self):
        path = self.bundle() / "bin/rust-runtime-app"
        with patch.object(data.runtime, "run", return_value=DYNAMIC + "libFabricRuntime.so"):
            with self.assertRaisesRegex(RuntimeError, "native-free ELF"):
                data.elf_identity(path, "native-free")
            self.assertEqual(data.elf_identity(path, "native-classic-dynamic")["classification"], "native-classic-dynamic")

    def test_nested_legacy_loader_remains_strict(self):
        directory = self.bundle()
        legacy = directory / "legacy/build-info.json"
        metadata = json.loads(legacy.read_text())
        metadata["extra"] = True
        legacy.write_text(json.dumps(metadata))
        self.mutate(directory, lambda outer: outer["artifacts"]["legacy/build-info.json"].update(
            sha256=data.runtime.digest(legacy)))
        with self.assertRaises(RuntimeError):
            data.load_bundle(directory)

    def test_artifact_drift_after_load_cannot_relabel_the_deployed_executable_hash(self):
        directory = self.bundle()
        bundle = data.load_bundle(directory)
        path = directory / "bin/rust-runtime-app"
        path.write_bytes(path.read_bytes() + b"changed after validation")
        with patch.object(data, "ROOT", self.root):
            with self.assertRaisesRegex(RuntimeError, "artifact drift"):
                data.package(bundle, "1" * 32, False)
        observer = directory / "bin/runtime_observer"
        observer.chmod(0o777)
        with self.assertRaisesRegex(RuntimeError, "artifact drift"):
            _ = bundle.observer

    def test_invalid_bundle_and_report_collision_fail_before_any_allocation_without_fallback(self):
        directory = self.bundle()
        (directory / "bin/runtime_observer").unlink()
        with patch.object(data.runtime, "create_owned") as create, \
             patch.object(data.runtime, "Allocation") as allocation, \
             patch.object(data, "build_bundle") as build:
            result = self.cli(directory)
            self.assertEqual(result["status"], "FAIL")
            create.assert_not_called()
            allocation.assert_not_called()
            build.assert_not_called()
        report = self.root / "already.json"
        report.write_text("original evidence")
        with patch.object(data, "load_bundle") as load, patch.object(data.runtime, "create_owned") as create:
            self.cli(directory, report="already.json")
            create.assert_not_called()
            load.assert_not_called()
        self.assertEqual(report.read_text(), "original evidence")

    def test_packaging_reads_graphs_without_compiling_and_rechecks_source_after(self):
        native = self.root / "target/debug/native-replicator-oracle"
        native.parent.mkdir(parents=True)
        native.write_bytes(executable("native-replicator-oracle"))
        def commands(*args, **kwargs):
            if args[:2] == ("readelf", "-d"):
                return DYNAMIC
            self.assertEqual(args[0], "cargo")
            self.assertEqual(args[1], "tree")
            self.assertIn("--locked", args)
            if args[1] == "tree":
                package = args[-1]
                return GRAPHS["runtime_observer" if package == "fube-client" else package]
            return ""
        with patch.object(data, "ROOT", self.root), \
             patch.object(data.runtime, "build_bundle", side_effect=self.make_legacy), \
             patch.object(data.runtime, "clean_source_revision", return_value="a" * 40) as clean, \
             patch.object(data.runtime, "run", side_effect=commands), \
             patch.object(data.runtime, "create_owned") as create:
            data.build_bundle(self.root / "built")
            create.assert_not_called()
            self.assertEqual(clean.call_count, 3)
            data.load_bundle(self.root / "built")
        with patch.object(data.runtime, "clean_source_revision", side_effect=RuntimeError("dirty")), \
             patch.object(data.runtime, "build_bundle") as build:
            with self.assertRaisesRegex(RuntimeError, "dirty"):
                data.build_bundle(self.root / "dirty")
            build.assert_not_called()
        self.assertFalse((self.root / "dirty").exists())
        with patch.object(data, "ROOT", self.root), \
             patch.object(data.runtime, "build_bundle", side_effect=self.make_legacy), \
             patch.object(data.runtime, "clean_source_revision", side_effect=["a" * 40, "b" * 40]), \
             patch.object(data.runtime, "run", side_effect=commands):
            with self.assertRaisesRegex(RuntimeError, "source changed"):
                data.build_bundle(self.root / "changed")
        self.assertFalse((self.root / "changed/build-info.json").exists())

    def test_new_package_preserves_rust_defaults_and_no_mixed_default_stateless(self):
        bundle = data.load_bundle(self.bundle())
        with patch.object(data, "ROOT", self.root):
            for mixed in (False, True):
                run_id = ("1" if mixed else "2") * 32
                package, name, version = data.package(bundle, run_id, mixed)
                manifest = ET.parse(package / "ApplicationManifest.xml")
                defaults = manifest.findall(f".//{{{data.runtime.NS}}}DefaultServices/{{{data.runtime.NS}}}Service")
                self.assertEqual([entry.get("Name") for entry in defaults], ["NativeBootstrap"] if mixed else ["Singleton", "Stateful"])
                if mixed:
                    bootstrap=defaults[0].find(f"{{{data.runtime.NS}}}StatefulService")
                    self.assertIsNotNone(bootstrap)
                    self.assertEqual(bootstrap.find(f"{{{data.runtime.NS}}}PlacementConstraints").text,
                                     "NodeType == N0010Ref")
                    self.assertEqual(bootstrap.get("TargetReplicaSetSize"),"1")
                    self.assertIsNone(defaults[0].find(f"{{{data.runtime.NS}}}StatelessService"))
                self.assertEqual(name, f"FubeData{run_id}Type")
                self.assertEqual(version, "1.1")
                service = ET.parse(package / "RustRuntimeService/ServiceManifest.xml")
                types = {entry.get("ServiceTypeName") for entry in service.find(f"{{{data.runtime.NS}}}ServiceTypes")}
                self.assertEqual(types, {"RustRuntimeType", "RustStatefulRuntimeType"})
                endpoints=service.find(f"{{{data.runtime.NS}}}Resources/{{{data.runtime.NS}}}Endpoints")
                self.assertEqual({entry.get("Name") for entry in endpoints},{"Rpc","Replication"})
                code = package / "RustRuntimeService/Code"
                self.assertEqual((code / "native-replicator-oracle").exists(), mixed)


class SelectorTests(ProjectFiles):
    def setUp(self):
        super().setUp()
        path = data.PKG / "launch-mixed.py"
        spec = importlib.util.spec_from_file_location("test_data_selector", path)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.record_startup = self.module.record_startup
        recorder = patch.object(self.module, "record_startup")
        recorder.start()
        self.addCleanup(recorder.stop)
        self.package = self.root / "RustRuntimeService.Code.1.1"
        self.package.mkdir()
        self.work = self.root / "work"
        self.work.mkdir()
        self.module.__file__ = str(self.package / "launch-mixed.py")
        for name in set(self.module.NODE_ENGINES.values()):
            target = self.package / name
            target.write_bytes(executable(name))
            target.chmod(0o674)
            self.module.EXPECTED_SHA256[name] = data.runtime.digest(target)
        self.addCleanup(os.chdir, Path.cwd())
        os.chdir(self.package)

    def environment(self, node="N0010"):
        return {"Fabric_NodeName": node, "Fabric_ApplicationHostId": "native-host",
                "Fabric_CodePackageInstanceSeqNum": "71", "Fabric_CodePackageName": "Code",
                "Fabric_Folder_App_Work": str(self.work), "CUSTOM": "preserved"}

    def test_unknown_or_missing_injected_node_never_execs_or_infers_role(self):
        for value in (None, "_Node_0", "N0000", "N0010\n", "N0060"):
            env = {} if value is None else {"Fabric_NodeName": value, "SF_NODE_NAME": "N0010"}
            with patch.dict(os.environ, env, clear=True), patch.object(os, "execve") as execute:
                with self.assertRaises(SystemExit):
                    self.module.main()
                execute.assert_not_called()

    def test_exact_static_map_execs_one_engine_preserving_all_activation_environment(self):
        self.assertEqual(self.module.NODE_ENGINES, {
            "N0010": "native-replicator-oracle", "N0020": "rust-runtime-app",
            "N0030": "native-replicator-oracle", "N0040": "rust-runtime-app", "N0050": "rust-runtime-app"})
        for node, binary in self.module.NODE_ENGINES.items():
            env = self.environment(node)
            target = self.package / binary
            before = target.stat().st_mode & 0o7777
            def execute(fd, arguments, environment):
                self.assertIsInstance(fd, int)
                self.assertEqual(os.fstat(fd).st_ino, target.stat().st_ino)
                self.assertEqual(arguments, [str(target)])
                self.assertEqual(environment, env)
            with patch.dict(os.environ, env, clear=True), \
                 patch.object(os, "execve", side_effect=execute) as invoked:
                self.module.main()
                invoked.assert_called_once()
            self.assertEqual(target.stat().st_mode & 0o7777, before | 0o100)

    def test_hash_mismatch_never_chmods_or_executes(self):
        target = self.package / "native-replicator-oracle"
        target.write_bytes(b"foreign bytes")
        with patch.dict(os.environ, self.environment(), clear=True), \
             patch.object(os, "fchmod") as chmod, patch.object(os, "execve") as execute:
            with self.assertRaisesRegex(SystemExit, "hash/identity mismatch"):
                self.module.main()
            chmod.assert_not_called()
            execute.assert_not_called()

    def test_nonregular_symlink_and_hardlinked_targets_are_refused(self):
        target = self.package / "native-replicator-oracle"
        for kind in ("directory", "fifo", "symlink", "hardlink"):
            target.unlink()
            unrelated = self.root / ("unrelated-" + kind)
            unrelated.write_bytes(executable(target.name))
            if kind == "directory":
                target.mkdir()
            elif kind == "fifo":
                os.mkfifo(target)
            elif kind == "symlink":
                target.symlink_to(unrelated)
            else:
                os.link(unrelated, target)
            before = unrelated.stat().st_mode
            with self.subTest(kind=kind), patch.dict(os.environ, self.environment(), clear=True), \
                 patch.object(os, "fchmod") as chmod, patch.object(os, "execve") as execute:
                with self.assertRaises((SystemExit, OSError)):
                    self.module.main()
                chmod.assert_not_called()
                execute.assert_not_called()
            self.assertEqual(unrelated.stat().st_mode, before)
            if kind == "directory":
                target.rmdir()
            else:
                target.unlink()
            target.write_bytes(executable(target.name))

    def test_foreign_nonexecutable_file_is_refused_without_modification(self):
        original = os.fstat
        def foreign(fd):
            value = original(fd)
            if not __import__("stat").S_ISREG(value.st_mode):
                return value
            fields = ("st_dev", "st_ino", "st_uid", "st_gid", "st_mode", "st_size",
                      "st_nlink", "st_mtime_ns", "st_ctime_ns")
            return types.SimpleNamespace(**{
                field: os.geteuid() + 1 if field == "st_uid" else getattr(value, field)
                for field in fields})
        with patch.dict(os.environ, self.environment(), clear=True), \
             patch.object(os, "fstat", side_effect=foreign), \
             patch.object(os, "access", return_value=False), \
             patch.object(os, "fchmod") as chmod, patch.object(os, "execve") as execute:
            with self.assertRaisesRegex(SystemExit, "not executable and is foreign-owned"):
                self.module.main()
            chmod.assert_not_called()
            execute.assert_not_called()

    def test_executable_runtime_owned_package_and_file_executes_without_chmod(self):
        original = os.fstat
        def foreign(fd):
            value = original(fd)
            fields = ("st_dev", "st_ino", "st_uid", "st_gid", "st_mode", "st_size",
                      "st_nlink", "st_mtime_ns", "st_ctime_ns")
            return types.SimpleNamespace(**{
                field: os.geteuid() + 1 if field == "st_uid" else getattr(value, field)
                for field in fields})
        target = self.package / "native-replicator-oracle"
        before = target.stat().st_mode
        with patch.dict(os.environ, self.environment(), clear=True), \
             patch.object(os, "fstat", side_effect=foreign), \
             patch.object(os, "access", return_value=True) as access, \
             patch.object(os, "fchmod") as chmod, patch.object(os, "execve") as execute:
            self.module.main()
            chmod.assert_not_called()
            execute.assert_called_once()
            self.assertTrue(all(call.kwargs == {"effective_ids": True}
                                for call in access.call_args_list))
        self.assertEqual(target.stat().st_mode, before)

    def test_owned_nonexecutable_file_adds_only_owner_execute(self):
        target = self.package / "native-replicator-oracle"
        target.chmod(0o640)
        with patch.dict(os.environ, self.environment(), clear=True), \
             patch.object(os, "access", side_effect=[False, True]), \
             patch.object(os, "fchmod", wraps=os.fchmod) as chmod, \
             patch.object(os, "execve") as execute:
            self.module.main()
            self.assertEqual(chmod.call_args.args[1], 0o740)
            self.assertEqual(target.stat().st_mode & 0o7777, 0o740)
            execute.assert_called_once()

    def test_already_executable_file_is_not_chmodded_and_unbound_hash_is_rejected(self):
        target = self.package / "native-replicator-oracle"
        target.chmod(0o754)
        with patch.dict(os.environ, self.environment(), clear=True), \
             patch.object(os, "fchmod") as chmod, patch.object(os, "execve") as execute:
            self.module.main()
            chmod.assert_not_called()
            execute.assert_called_once()
            execute.reset_mock()
            self.module.EXPECTED_SHA256[target.name] = "@unbuilt:sha256@"
            with self.assertRaisesRegex(SystemExit, "no strict prebuilt"):
                self.module.main()
            execute.assert_not_called()

    def test_unrelated_activation_directory_is_refused(self):
        for field, value in (("Fabric_Folder_App_Work", "/foreign/work"),
                             ("Fabric_CodePackageName", "Other")):
            env = self.environment()
            env[field] = value
            with patch.dict(os.environ, env, clear=True), patch.object(os, "fchmod") as chmod, \
                 patch.object(os, "execve") as execute:
                with self.assertRaisesRegex(SystemExit, "activation-owned"):
                    self.module.main()
                chmod.assert_not_called()
                execute.assert_not_called()

    def test_startup_diagnostic_records_actual_caller_once_without_overwriting(self):
        path = self.work / f"RustRuntimeService.FubeDataStartup.{os.getpid()}.stderr"
        with patch.object(os, "dup2") as redirect:
            self.record_startup(self.work, "native-replicator-oracle", os.getuid() + 1, 0o674, False)
            self.assertEqual(redirect.call_count, 2)
            before = path.read_bytes()
            context = json.loads(before.decode())
            self.assertEqual(context["uid"], os.getuid())
            self.assertEqual(context["euid"], os.geteuid())
            self.assertEqual(context["gid"], os.getgid())
            self.assertEqual(context["egid"], os.getegid())
            self.assertEqual(context["groups"], os.getgroups())
            self.assertIs(context["mode_changed"], False)
            redirect.reset_mock()
            self.record_startup(self.work, "rust-runtime-app", os.getuid(), 0o740, True)
            redirect.assert_not_called()
            self.assertEqual(path.read_bytes(), before)


def observation(mixed=True, target=3):
    epoch = {"data_loss": 1, "configuration": 2 << 32}
    replicas = []
    for index, node in enumerate(("N0010", "N0020", "N0030")[:target]):
        kind = data.NODE_ENGINES[node] if mixed else "rust"
        role = "Primary" if index == 0 else "ActiveSecondary"
        observed_epoch = {"data_loss_number": epoch["data_loss"],
                          "configuration_number": epoch["configuration"]}
        rpc = {
            "profile": "fube-data-v1", "engine_kind": kind, "partition": "partition", "replica": index + 10,
            "node": node, "host": "host-" + node, "pid": 100 + index, "incarnation": index + 1000,
            "engine_incarnation": "engine-" + node, "service_address": f"http://127.0.0.1:{2000+index}/data/owner/service",
            "role": role, "current_role": role, "native_epoch": observed_epoch,
            "replicator_role": None if kind == "native-classic-v1" else role,
            "replication_address": None if kind == "native-classic-v1" else "127.0.0.1:3000+actor",
            "engine": {"copy_done": True, "closed": False, "fault": None, "peers": [], "progress": {"committed": 0}},
            "pump_fault": None, "read_status": "Granted" if index == 0 else "NotPrimary",
            "write_status": "Granted" if index == 0 else "NotPrimary",
            "state": {"copy_in_progress": False, "last": 0, "results": {}, "values": {}},
            "native_settings": {"require_service_ack": 1, "flags": 32},
            "requested_native_settings": {"require_service_ack": True, "require_service_ack_flag": 32},
            "native_settings_error": None,
        }
        replicas.append({"node": node, "replica": index + 10, "role": role, "status": "Ready",
            "rpc": rpc, "deployed": {"host_process_id": 100 + index, "service_type_name": "RustStatefulRuntimeType"},
            "address": rpc["service_address"], "service_http_status": 200 if index == 0 else 503})
    return {
        "observer": "fube-client", "service": "fabric:/Owned/Data", "partition": "partition",
        "native_partition": {"target": target, "minimum": 2 if target == 3 else 1, "status": "Ready", "epoch": epoch},
        "replicas": replicas, "primary": {"resolver": "fube-client", "endpoint": replicas[0]["address"],
            "native_pid": 100, "rpc": copy.deepcopy(replicas[0]["rpc"])},
    }


class NativeIdentityTests(unittest.TestCase):
    def test_current_oracle_epoch_schema_for_native_primary_and_secondary(self):
        for target, index in ((1, 0), (3, 2)):
            with self.subTest(target=target, index=index):
                value = observation(True, target)
                rpc = value["replicas"][index]["rpc"]
                self.assertEqual(rpc["engine_kind"], "native-classic-v1")
                # Exact host.rs diagnostics shape; FM authority keeps its own
                # independently obtained data_loss/configuration vocabulary.
                rpc["native_epoch"] = {
                    "data_loss_number": 1,
                    "configuration_number": 2 << 32,
                }
                data.assert_data(value, value["service"], True, target,
                                 1 if target == 1 else 2)
                rpc["native_epoch"] = {"data_loss": 1, "configuration": 2 << 32}
                with self.assertRaises(RuntimeError):
                    data.assert_data(value, value["service"], True, target,
                                     1 if target == 1 else 2)

    def test_native_secondary_callback_epoch_can_remain_unknown_after_real_progress(self):
        value=observation()
        rpc=value["replicas"][2]["rpc"]
        rpc["native_epoch"]=None
        rpc["native_current_progress"]=0
        rpc["engine"]["progress"]["received"]=0
        data.assert_data(value,value["service"],True)
        rpc["state"]["last"]=1
        rpc["state"]["values"]={"key":"value"}
        rpc["state"]["results"]={"request":1}
        rpc["native_current_progress"]=1
        rpc["engine"]["progress"]["received"]=1
        data.assert_data(value,value["service"],True)
        self.assertIsNone(rpc["native_epoch"])
        rpc["native_epoch"]={"data_loss_number":1,"configuration_number":1 << 32}
        data.assert_data(value,value["service"],True)
        rpc["native_epoch"]={"data_loss_number":2,"configuration_number":2 << 32}
        with self.assertRaises(RuntimeError):
            data.assert_data(value,value["service"],True)
        for index in (0,1):
            current=observation()
            current["replicas"][index]["rpc"]["native_epoch"]=None
            with self.assertRaises(RuntimeError):
                data.assert_data(current,current["service"],True)

    def test_primary_refresh_allows_real_quorum_while_replacement_is_in_build(self):
        value=observation()
        value["replicas"][1]={"node":"N0020","replica":11,"role":"IdleSecondary","status":"InBuild","address":""}
        with self.assertRaises(RuntimeError):
            data.assert_data(value,value["service"],True)
        data.assert_data(value,value["service"],True,require_full=False)
        # This is availability only, never a full mixed-partition qualification.
        value["replicas"][2]["rpc"]["write_status"]="NoWriteQuorum"
        value["primary"]["rpc"]["write_status"]="NoWriteQuorum"
        value["replicas"][0]["rpc"]["write_status"]="NoWriteQuorum"
        with self.assertRaises(RuntimeError):
            data.assert_data(value,value["service"],True,require_full=False)

    def test_real_native_and_rust_roles_and_null_native_endopen_are_distinct(self):
        value = observation()
        self.assertIs(data.assert_data(value, value["service"], True), value)
        self.assertIsNone(value["replicas"][0]["rpc"]["replication_address"])
        rust = observation(False)
        data.assert_data(rust, rust["service"], False)
        native_singleton = observation(True, 1)
        data.assert_data(native_singleton, native_singleton["service"], True, 1, 1)

    def test_mismatched_deployment_process_service_role_epoch_and_ack_flag_fail_closed(self):
        mutations = [
            lambda value: value["replicas"][0]["deployed"].update(host_process_id=999),
            lambda value: value["primary"].update(native_pid=999),
            lambda value: value["replicas"][1]["rpc"].update(engine_kind="native-classic-v1"),
            lambda value: value["replicas"][1]["rpc"].update(partition="wrong"),
            lambda value: value["replicas"][0]["rpc"].update(replication_address="invented"),
            lambda value: value["replicas"][0]["rpc"]["native_settings"].update(require_service_ack=0),
            lambda value: value["replicas"][0]["rpc"]["native_settings"].update(flags=0),
            lambda value: value["replicas"][1]["rpc"]["native_epoch"].update(configuration_number=9 << 32),
            lambda value: value["replicas"][1]["rpc"].update(role="Secondary"),
            lambda value: value["replicas"][1]["rpc"].update(current_role="Secondary"),
            lambda value: value["replicas"][1]["rpc"].update(replicator_role="Secondary"),
            lambda value: value["replicas"][1]["rpc"].update(role="IdleSecondary"),
            lambda value: value["replicas"][1]["rpc"].update(role="Idle"),
            lambda value: value["replicas"][1].update(service_http_status=200),
            lambda value: value["replicas"][0]["rpc"].update(write_status="NoWriteQuorum"),
        ]
        for action in mutations:
            value = observation()
            action(value)
            with self.assertRaises(RuntimeError):
                data.assert_data(value, value["service"], True)

    def test_initialization_is_sdk_integer_bytes_not_base64_and_placement_is_real_nodes(self):
        description = data.service_description("fabric:/Owned", "fabric:/Owned/Data", data.MIXED_NODES)
        self.assertEqual(description["InitializationData"], list(b"fube-data-v1"))
        self.assertEqual(description["PartitionDescription"], {"PartitionScheme": "Singleton"})
        self.assertFalse(description["HasPersistedState"])
        self.assertEqual(description["PlacementConstraints"],
                         "NodeName == N0010 || NodeName == N0020 || NodeName == N0030")
        allocation = MagicMock()
        allocation.execute.return_value = '{"http_status":202,"body":""}'
        data.create_data_service(allocation, "fabric:/Owned", "fabric:/Owned/Data", data.MIXED_NODES)
        args = allocation.execute.call_args.args
        self.assertEqual(args[:2], ("python3", "-c"))
        self.assertIn("/Applications/Owned/$/GetServices/$/Create?api-version=6.0", args)
        self.assertEqual(json.loads(args[-1]), description)
        allocation.sf.assert_not_called()

    def test_no_http_probe_can_target_a_classic_tcp_control_address(self):
        for endpoint in ("127.0.0.1:32000+actor", "tcp://127.0.0.1:32000/",
                         "http://127.0.0.1:32000/not-service", "http://user@127.0.0.1:2/owner/service"):
            with self.assertRaises(RuntimeError):
                data.request(endpoint, "diagnostics")

    def test_process_expected_comes_from_native_code_query_not_self_reported_exe(self):
        value = observation(False)
        replica = value["replicas"][0]
        package = {"Name": "Code", "ServiceManifestName": "RustRuntimeService", "Version": "1.1:1",
                   "MainEntryPoint": {"ProcessId": 100, "InstanceId": 77,
                                      "EntryPointLocation": "/native/deployed/rust-runtime-app"}}
        bundle = MagicMock()
        bundle.binary.return_value = Path("/prebuilt/rust-runtime-app")
        bundle.directory = Path("/prebuilt")
        bundle.metadata = {"artifacts": {
            "bin/rust-runtime-app": {"sha256": "a" * 64},
            "launchers/launch-rust.py": {"sha256": "b" * 64},
        }}
        with patch.object(data.runtime, "native_read", return_value=[package]), \
             patch.object(data.runtime, "digest", return_value="a" * 64):
            expected = data.process_expected(MagicMock(), bundle, value["service"], replica, False)
            self.assertEqual(expected["native_entry_point"], "/native/deployed/rust-runtime-app")
            self.assertEqual(expected["engine_name"], "rust-runtime-app")
            self.assertEqual(expected["native_entry_point_instance"], 77)
            self.assertEqual(expected["engine_sha256"], "a" * 64)
            self.assertEqual(expected["launcher_sha256"], "a" * 64)
            package["MainEntryPoint"]["ProcessId"] = "100"
            self.assertEqual(data.process_expected(
                MagicMock(), bundle, value["service"], replica, False)["native_pid"], 100)
            package["MainEntryPoint"]["ProcessId"] = 999
            with self.assertRaises(RuntimeError):
                data.process_expected(MagicMock(), bundle, value["service"], replica, False)

    def test_native_rest_pid_normalization_preserves_strict_identity(self):
        for value in (1, 100, 2**31 - 1, "1", "100", str(2**31 - 1)):
            self.assertEqual(data.native_process_id(value), int(value))
        for value in (None, True, False, 0, -1, 2**31, "0", "-1", "+1", " 1", "1 ",
                      "01", "1.0", "1\n", str(2**31), "9" * 100):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                data.native_process_id(value)


class CopyAckGateTests(unittest.TestCase):
    def test_no_write_native_acks_require_actual_receipts_not_descriptor_prefix_or_new_assignments(self):
        value = observation()
        source = value["primary"]["rpc"]
        source["engine_kind"] = "rust"
        source["state"]["last"] = 20
        source["engine"]["progress"].update(assigned=20, committed=20)
        expected = [replica["replica"] for replica in value["replicas"] if replica["role"] == "ActiveSecondary"]
        source["engine"]["peers"] = [{"replica": replica, "received": 20, "applied": 20,
            "native_catchup_prefix": 20, "building": False, "cleanup_required": False} for replica in expected]
        self.assertTrue(data.no_write_native_acks(value, 20))
        for field in ("received", "applied"):
            altered = copy.deepcopy(value)
            altered["primary"]["rpc"]["engine"]["peers"][0][field] = 0
            self.assertFalse(data.no_write_native_acks(altered, 20))
        altered = copy.deepcopy(value)
        altered["primary"]["rpc"]["engine"]["progress"]["assigned"] = 21
        self.assertFalse(data.no_write_native_acks(altered, 20))
        altered = copy.deepcopy(value)
        altered["primary"]["rpc"]["engine"]["peers"].pop()
        self.assertFalse(data.no_write_native_acks(altered, 20))

    def fixture(self, primary_kind):
        before = observation()
        primary_index = 1 if primary_kind == "rust" else 0
        destination_index = 2 if primary_kind == "rust" else 1
        for index, replica in enumerate(before["replicas"]):
            replica["role"] = "Primary" if index == primary_index else "ActiveSecondary"
        removed = copy.deepcopy(before["replicas"][destination_index])
        value = copy.deepcopy(before)
        destination = value["replicas"][destination_index]
        destination.update(replica=30, role="IdleSecondary", status="InBuild", address="")
        rpc = copy.deepcopy(removed["rpc"])
        rpc.update(replica=30, incarnation="new", engine_incarnation="new-engine",
                   service_address=removed["address"].replace("/owner/", "/replacement/"))
        rpc["state"].update(last=5, results={"copied": 5}, values={"key": "value"})
        rpc["engine"].update(copy_done=False, ready=False)
        rpc["engine"]["progress"]["applied"] = 0
        rpc["copy_ack_hold"] = {"armed_ms": 6000, "held": True, "sequence": 7, "boundary": 5,
            "copy_end_received": True, "released": False, "acknowledged": False,
            "expired": False, "cancelled": False}
        primary = value["replicas"][primary_index]["rpc"]
        primary["engine"]["peers"] = [{"replica": 30, "copy_horizon": 5, "copy_received": -1,
            "copy_applied": 6, "copy_finish": 8, "applied": 0, "building": True}]
        primary["native_copy_horizons"] = [{"requested_horizon": 5, "enumeration_end": True}]
        primary["state"]["last"] = 8
        qualification = data.Qualification(MagicMock(), MagicMock(), {"application": "fabric:/Owned"}, True)
        evidence = {}
        return data.CopyAckGate(qualification, before, removed, evidence), value, destination, rpc

    def test_both_native_directions_require_bracketed_inbuild_then_explicit_release_and_same_ready_ledger(self):
        for primary_kind in ("rust", "native-classic-v1"):
            gate, value, destination, rpc = self.fixture(primary_kind)
            gate.qualification.hook = MagicMock(return_value={"qualification_only": True})
            with patch.object(data, "request", return_value=(200, rpc)), \
                 patch.object(data.time, "monotonic", side_effect=[1, 2, 3.2, 3.3]):
                gate.observe(value, 0)
                self.assertEqual(gate.qualification.hook_identities[gate.endpoint],
                    {"node": destination["node"], "replica": destination["replica"],
                     "service": gate.qualification.service})
                self.assertFalse(gate.released)
                gate.observe(value, 1.1)
                self.assertFalse(gate.released, "one negative native query is insufficient")
                gate.observe(value, 2.1)
            self.assertTrue(gate.released)
            self.assertEqual(gate.evidence["negative_observations"], 2)
            self.assertEqual([call.args[1:] for call in gate.qualification.hook.call_args_list],
                             [("copy_ack_hold_ms", 6000), ("copy_ack_hold_ms", 0)])
            recovered = copy.deepcopy(value)
            ready = next(r for r in recovered["replicas"] if r["replica"] == 30)
            ready.update(status="Ready", rpc=copy.deepcopy(rpc))
            ready["rpc"]["copy_ack_hold"].update(held=False, released=True, acknowledged=True)
            ready["rpc"]["engine"]["copy_done"] = True
            ready["rpc"]["state"]["last"] = 8
            gate.complete(recovered, {"last": 8})
            ready["replica"] = 31
            with self.assertRaisesRegex(RuntimeError, "same native replica"):
                gate.complete(recovered, {"last": 8})

    def test_receive_eos_without_actual_held_ack_or_native_not_ready_never_passes(self):
        for primary_kind in ("rust", "native-classic-v1"):
            gate, value, replica, rpc = self.fixture(primary_kind)
            data.CopyAckGate.negative(value, replica, rpc)
            for key, changed in (("held", False), ("copy_end_received", False), ("acknowledged", True),
                                 ("released", True), ("boundary", 0), ("sequence", None)):
                altered = copy.deepcopy(rpc)
                altered["copy_ack_hold"][key] = changed
                with self.subTest(key=key), self.assertRaises(data.MissingEvidence):
                    data.CopyAckGate.negative(value, replica, altered)
            for key in ("expired", "cancelled"):
                altered = copy.deepcopy(rpc)
                altered["copy_ack_hold"][key] = True
                with self.assertRaises(RuntimeError):
                    data.CopyAckGate.negative(value, replica, altered)
            replica["status"] = "Ready"
            with self.assertRaisesRegex(RuntimeError, "before destination copy ACK release"):
                data.CopyAckGate.negative(value, replica, rpc)
            with self.assertRaises(data.MissingEvidence):
                gate.complete(value, {"last": 8})

    def test_missing_source_eos_and_receive_only_source_completion_are_refused(self):
        for primary_kind in ("rust", "native-classic-v1"):
            _, value, replica, rpc = self.fixture(primary_kind)
            primary = next(r["rpc"] for r in value["replicas"] if r["role"] == "Primary")
            if primary_kind == "rust":
                primary["engine"]["peers"][0]["copy_applied"] = -1
                with self.assertRaisesRegex(RuntimeError, "receive-only"):
                    data.CopyAckGate.negative(value, replica, rpc)
                primary["engine"]["peers"][0]["copy_received"] = 6
            else:
                primary["native_copy_horizons"][0]["enumeration_end"] = False
            with self.assertRaises(data.MissingEvidence):
                data.CopyAckGate.negative(value, replica, rpc)

    def test_inbuild_discovery_and_release_never_rebind_to_a_different_incarnation(self):
        gate, value, _, rpc = self.fixture("rust")
        gate.qualification.hook = MagicMock()
        drifted = copy.deepcopy(rpc)
        drifted["incarnation"] = "different"
        with patch.object(data, "request", side_effect=[(200, rpc), (200, drifted)]):
            with self.assertRaisesRegex(RuntimeError, "lifetime drifted"):
                gate.observe(value, 0)
        self.assertFalse(gate.released)
        gate.qualification.hook.assert_called_once()

    def test_actual_direction_scenarios_cannot_omit_destination_copy_ack_gate(self):
        for kind in ("rust", "native-classic-v1"):
            gate, value, _, _ = self.fixture(kind)
            value = copy.deepcopy(gate.before)
            primary = next(r for r in value["replicas"] if r["role"] == "Primary")
            value["primary"]["rpc"] = primary["rpc"]
            for replica in value["replicas"]:
                if replica["rpc"]["engine_kind"] == "rust":
                    replica["rpc"]["observed_primary"] = "real-native-selected-peer"
            q = gate.qualification
            with patch.object(q, "wait", return_value=value), patch.object(q, "move", return_value=value), \
                 patch.object(q, "writes"), patch.object(q, "apply_ack_delay"), \
                 patch.object(q, "copy") as copying, patch.object(q, "converge", return_value=value):
                q.direction(kind, {})
            copying.assert_called_once()
            self.assertIs(copying.call_args.kwargs["hold_copy_ack"], True)


class WorkloadTests(unittest.TestCase):
    def test_ambiguous_retry_keeps_sequence_and_never_undoes_or_creates_new_identity(self):
        ledger = data.Ledger()
        record = ledger.mutation("operation")
        ledger.reply(record, 202, {"request_id": "operation", "outcome": "ambiguous", "sequence": 7})
        ledger.reply(record, 200, {"request_id": "operation", "outcome": "committed", "sequence": 7})
        self.assertTrue(record["committed"])
        self.assertEqual(record["sequence"], 7)
        with self.assertRaisesRegex(RuntimeError, "changed"):
            ledger.reply(record, 200, {"request_id": "operation", "outcome": "committed", "sequence": 8})
        with self.assertRaises(RuntimeError):
            ledger.reply(record, 503, {"request_id": "operation", "outcome": "rejected", "sequence": 7})

    def test_independent_map_ledger_comparison_requires_every_successful_id_and_value(self):
        ledger = data.Ledger()
        record = ledger.mutation("id-1")
        ledger.reply(record, 200, {"request_id": "id-1", "outcome": "committed", "sequence": 9})
        value = observation()
        for replica in value["replicas"]:
            replica["rpc"]["state"].update(last=9, results={"id-1": 9}, values={record["key"]: record["value"]})
        self.assertEqual(ledger.compare(value)["successful"], 1)
        value["replicas"][1]["rpc"]["state"]["results"] = {}
        with self.assertRaises(RuntimeError):
            ledger.compare(value)

    def test_client_caches_primary_and_refreshes_only_on_definite_rejection_or_ambiguous_poll(self):
        ledger = data.Ledger()
        client = data.Client(MagicMock(), "fabric:/Owned/Data", False, ledger)
        client.endpoint = "http://old:20/data/owner/service"
        def refresh():
            client.endpoint = "http://new:21/data/owner/service"
        replies = [
            (503, {"request_id": "one", "outcome": "rejected"}),
            (202, {"request_id": "one", "outcome": "ambiguous", "sequence": 2}),
            (200, {"request_id": "one", "outcome": "committed", "sequence": 2}),
            (200, {"request_id": "two", "outcome": "committed", "sequence": 3}),
        ]
        with patch.object(data, "request", side_effect=replies) as request, \
             patch.object(client, "refresh", side_effect=refresh) as resolver, \
             patch.object(data.time, "sleep"):
            client.write("one")
            client.write("two")
        self.assertEqual(resolver.call_count, 2)
        self.assertEqual([call.args[2]["id"] for call in request.call_args_list], ["one", "one", "one", "two"])
        self.assertEqual(request.call_args_list[0].args[0], "http://old:20/data/owner/service")
        self.assertEqual(request.call_args_list[-1].args[0], "http://new:21/data/owner/service")

    def test_copy_evidence_requires_native_inbuild_and_observed_not_invented_horizons(self):
        removed = {"node": "N0010", "replica": 10}
        value = observation()
        value["replicas"][0].update(status="InBuild", replica=30)
        value["replicas"][1]["rpc"]["engine"]["peers"] = [
            {"replica": 30, "copy_horizon": 100, "copy_finish": 200, "address": "native-control+actor"}]
        evidence = data.Qualification.copy_evidence([{"at_monotonic": 10, "native": value}], removed)
        self.assertEqual(evidence["native_in_build"][0]["replica"], 30)
        self.assertEqual(evidence["horizons"][0]["copy_horizon"], 100)
        value["replicas"][1]["rpc"]["engine"]["peers"] = []
        self.assertFalse(data.Qualification.copy_evidence([{"at_monotonic": 10, "native": value}], removed)["horizons"])

    def test_no_force_remove_and_native_bound_partition_replica_node(self):
        allocation = MagicMock()
        result = {"application": "fabric:/Owned"}
        qualification = data.Qualification(allocation, MagicMock(), result, True)
        value = observation()
        qualification.remove(value, value["replicas"][1])
        args = allocation.sf.call_args.args
        self.assertEqual(args, ("replica", "remove", "--node-name", "N0020", "--partition-id", "partition",
                                "--replica-id", "11"))
        self.assertNotIn("--force-remove", args)

    def test_capacity_rejection_preserves_an_old_committed_identity_even_if_retry_is_ambiguous(self):
        qualification = data.Qualification(MagicMock(), MagicMock(), {"application": "fabric:/Owned"}, False)
        original = qualification.ledger.mutation("original")
        qualification.ledger.reply(original, 200, {
            "request_id": "original", "outcome": "committed", "sequence": 1})
        full = observation(False)
        full["primary"]["rpc"]["state"].update(
            results={str(index): index for index in range(1, 4097)},
            history_count=4096, retained_bytes=5 * 1024 * 1024)
        rejected = (503, {"request_id": "ledger-capacity-0", "outcome": "rejected",
                          "error": "resource limit: provider history/identity ledger"})
        retry = (202, {"request_id": "original", "outcome": "ambiguous", "duplicate": True, "sequence": 1})
        evidence = {}
        with patch.object(qualification, "converge", return_value=full), \
             patch.object(data, "request", side_effect=[rejected, retry]) as request:
            qualification.ledger_capacity_backpressure(evidence)
        self.assertEqual(request.call_count, 2)
        self.assertTrue(original["committed"])
        self.assertEqual(original["sequence"], 1)
        self.assertIsNone(qualification.ledger.records["ledger-capacity-0"]["sequence"])
        self.assertEqual(evidence["old_identity_retry_at_capacity"]["outcome"], "ambiguous")

    def test_no_write_quorum_is_not_mislabeled_as_capacity_backpressure(self):
        qualification = data.Qualification(MagicMock(), MagicMock(), {"application": "fabric:/Owned"}, False)
        with patch.object(qualification, "converge", return_value=observation(False)), \
             patch.object(data, "request", return_value=(503, {
                 "request_id": "ledger-capacity-0", "outcome": "rejected", "error": "NoWriteQuorum"})):
            with self.assertRaisesRegex(RuntimeError, "not actual provider capacity"):
                qualification.ledger_capacity_backpressure({})

    def test_native_inbuild_gate_cancellation_joins_worker_without_sending_writes(self):
        qualification = data.Qualification(MagicMock(), MagicMock(), {"application": "fabric:/Owned"}, False)
        with patch.object(qualification, "writes") as writes:
            with qualification.writing(200, "copy", threading.Event()) as worker:
                pass
        self.assertTrue(worker.done())
        self.assertIsInstance(worker.exception(), RuntimeError)
        writes.assert_not_called()


class ProcessBindingTests(unittest.TestCase):
    def test_digest_works_on_native_python310_without_file_digest(self):
        with patch.object(hashlib, "file_digest", side_effect=AssertionError("not on Python 3.10"),
                          create=True):
            evidence = self.probe(self.expected(), action="inspect")
        self.assertIsNotNone(evidence)
        self.assertEqual(evidence["executable_sha256"], hashlib.sha256(b"engine").hexdigest())

    def expected(self, kind="rust"):
        engine = "rust-runtime-app" if kind == "rust" else "native-replicator-oracle"
        launcher = "launch-rust.py" if kind == "rust" else "launch-mixed.py"
        return {
            "native_pid": 321, "native_entry_point": "/native/deployed/" + launcher,
            "launcher_name": launcher, "launcher_sha256": hashlib.sha256(b"selector").hexdigest(),
            "engine_name": engine, "engine_sha256": hashlib.sha256(b"engine").hexdigest(),
            "host": "native-host", "application": "fabric:/Owned", "node": "N0010", "kind": kind,
            "captured": {"pid": 321, "exe": "/native/deployed/" + engine, "start_ticks": 81,
                         "code_sequence": 71, "host": "native-host",
                         "launcher_sha256": hashlib.sha256(b"selector").hexdigest(),
                         "executable_sha256": hashlib.sha256(b"engine").hexdigest()},
        }

    def probe(self, expected, *, host="native-host", engine=b"engine", launcher=b"selector",
              mappings=("/usr/lib/libc.so.6",), start=81, action="kill", stale=False):
        engine_path = "/native/deployed/" + expected["engine_name"]
        class FakePath:
            def __init__(self, value):
                self.value = str(value)
            def __truediv__(self, value):
                return FakePath(self.value + "/" + str(value))
            def __str__(self):
                return self.value
            @property
            def name(self):
                return self.value.rsplit("/", 1)[-1]
            @property
            def parent(self):
                return FakePath(self.value.rsplit("/", 1)[0])
            def is_absolute(self):
                return self.value.startswith("/")
            def is_symlink(self):
                return False
            def is_file(self):
                return True
            def read_bytes(self):
                return (f"Fabric_ApplicationHostId={host}\0Fabric_ApplicationName=fabric:/Owned\0"
                        "Fabric_NodeName=N0010\0Fabric_ServicePackageName=RustRuntimeService\0"
                        "Fabric_CodePackageInstanceSeqNum=71\0").encode()
            def read_text(self):
                if self.value.endswith("/stat"):
                    fields = ["T" if action == "pause" else "S", "119"] + ["0"] * 17 + [str(start)] + ["0"] * 8
                    return "321 (test engine) " + " ".join(fields)
                return "\n".join(f"1-2 r-xp 0 0 0 {path}" for path in mappings)
            def open(self, mode):
                if mode != "rb":
                    raise AssertionError("unexpected mode")
                return io.BytesIO(engine if self.value.endswith("/exe") else launcher)
        with patch("sys.argv", ["process", json.dumps(expected), action]), \
             patch("pathlib.Path", FakePath), \
             patch("os.pidfd_open", side_effect=ProcessLookupError if stale else None, return_value=44), \
             patch("os.readlink", return_value=engine_path), patch("os.close"), \
             patch("signal.pidfd_send_signal") as signal, patch("select.poll") as poll, \
             contextlib.redirect_stdout(io.StringIO()) as output:
            poll.return_value.poll.return_value = [(44, 1)]
            try:
                exec(data.DATA_PROCESS, {})
            except (RuntimeError, ProcessLookupError):
                signal.assert_not_called()
                return None
            if action in ("kill", "pause", "resume"):
                signal.assert_called_once_with(44, {"kill": 9, "pause": 19, "resume": 18}[action])
            else:
                signal.assert_not_called()
            return json.loads(output.getvalue())

    def test_pidfd_kill_requires_native_launcher_engine_host_and_code_incarnation(self):
        expected = self.expected()
        evidence = self.probe(expected)
        self.assertTrue(evidence["old_process_exit_confirmed"])
        self.assertEqual(evidence["classification"], "native-free")
        for kwargs in ({"host": "other-host"}, {"engine": b"replaced"},
                       {"launcher": b"changed-selector"}, {"start": 82}, {"stale": True}):
            with self.subTest(kwargs=kwargs):
                self.assertIsNone(self.probe(expected, **kwargs))
        changed = copy.deepcopy(expected)
        changed["captured"]["code_sequence"] = 72
        self.assertIsNone(self.probe(changed))

    def test_classification_is_per_process_and_never_relaxes_rust_for_native_peer(self):
        for library in ("libFabricRuntime.so", "libFabricCommon.so", "libFabricClient.so"):
            self.assertIsNone(self.probe(self.expected(), mappings=("/opt/" + library,)))
        native = self.expected("native-classic-v1")
        self.assertIsNone(self.probe(native))
        evidence = self.probe(native, mappings=(
            "/opt/libFabricRuntime.so", "/opt/libFabricCommon.so", "/opt/libFabricClient.so"))
        self.assertEqual(evidence["classification"], "native-classic-dynamic")
        self.assertTrue(all(evidence["native_libraries"].values()))

    def test_mutation_without_prior_captured_identity_never_signals_a_process(self):
        expected = self.expected()
        del expected["captured"]
        self.assertIsNone(self.probe(expected))
        self.assertEqual(self.probe(expected, action="inspect")["code_sequence"], 71)

    def test_all_mutations_require_complete_well_typed_prior_capture(self):
        malformed = ({}, False, 0, [], "", {"host": "native-host"},
                     {"start_ticks": True, "code_sequence": 71, "host": "native-host"},
                     {"start_ticks": 81, "code_sequence": True, "host": "native-host"},
                     {"start_ticks": 81, "code_sequence": -1, "host": "native-host"},
                     {"start_ticks": 81, "code_sequence": 71, "host": 3},
                     {"start_ticks": 81, "code_sequence": 71, "host": ""})
        for action in ("kill", "pause", "resume"):
            for capture in malformed:
                with self.subTest(action=action, capture=capture):
                    expected = self.expected()
                    expected["captured"] = ({**expected["captured"], **capture}
                                            if isinstance(capture, dict) and len(capture) > 1 else capture)
                    self.assertIsNone(self.probe(expected, action=action))

    def test_pause_resume_require_original_start_code_host_and_bind_the_pidfd(self):
        for action in ("pause", "resume"):
            with self.subTest(action=action):
                evidence = self.probe(self.expected(), action=action)
                self.assertEqual(evidence["action"], action)
                for kwargs in ({"start": 82}, {"host": "replacement"}, {"stale": True}):
                    self.assertIsNone(self.probe(self.expected(), action=action, **kwargs))
                changed = self.expected()
                changed["captured"]["code_sequence"] = 72
                self.assertIsNone(self.probe(changed, action=action))
                for field, value in (("pid", 322), ("exe", "/foreign/engine"),
                                     ("launcher_sha256", "0" * 64), ("executable_sha256", "0" * 64)):
                    changed = self.expected()
                    changed["captured"][field] = value
                    self.assertIsNone(self.probe(changed, action=action))

    def test_resume_keeps_paused_owner_on_malformed_capture_or_replacement_without_recapturing(self):
        for malformed in (False, True):
            qualification = data.Qualification(MagicMock(), MagicMock(), {"application": "fabric:/Owned"}, False)
            expected = self.expected()
            if malformed:
                expected["captured"] = {}
            qualification.paused.append(expected)
            def resume(_allocation, original, action):
                self.assertIs(original, expected)
                self.assertEqual(action, "resume")
                value = self.probe(original, action=action, start=81 if malformed else 82)
                if value is None:
                    raise RuntimeError("original captured identity refused")
                return value
            with patch.object(data, "inspect_process", side_effect=resume):
                evidence = qualification.resume()
            self.assertEqual(qualification.paused, [expected])
            self.assertIn("original captured identity refused", evidence[0]["error"])
            self.assertEqual(expected["captured"], {} if malformed else self.expected()["captured"])
        qualification = data.Qualification(MagicMock(), MagicMock(), {"application": "fabric:/Owned"}, False)
        expected = self.expected()
        qualification.paused.append(expected)
        with patch.object(data, "inspect_process", side_effect=lambda _, original, action:
                          self.probe(original, action=action)):
            evidence = qualification.resume()
        self.assertEqual(qualification.paused, [])
        self.assertEqual(evidence[0]["action"], "resume")


class OwnershipTests(ProjectFiles):
    def test_interrupted_readiness_persists_report_and_cleans_only_original_capability(self):
        def create(name, *, on_allocated):
            on_allocated("a" * 32)
            raise KeyboardInterrupt
        with patch.object(data, "load_bundle", return_value=MagicMock()), \
             patch.object(data.runtime, "create_owned", side_effect=create), \
             patch.object(data.runtime, "Allocation") as allocation, \
             patch.object(data.runtime, "destroy_owned", return_value={"absence_verified": True}) as destroy:
            result = self.cli(self.root / "unused")
        allocation.assert_not_called()
        destroy.assert_called_once_with("owned", "a" * 32)
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["original_allocation_owner"], "a" * 32)
        self.assertIn("KeyboardInterrupt", result["errors"][0])
        self.assertTrue(result["cleanup"]["onebox"]["absence_verified"])

    def test_original_capability_survives_partial_readiness_failure(self):
        def create(name, *, on_allocated):
            on_allocated("a" * 32)
            raise RuntimeError("readiness failed after committed allocation")
        with patch.object(data, "load_bundle", return_value=MagicMock()), \
             patch.object(data.runtime, "create_owned", side_effect=create), \
             patch.object(data.runtime, "Allocation") as allocation, \
             patch.object(data.runtime, "destroy_owned", return_value={"absence_verified": True}) as destroy:
            result = self.cli(self.root / "unused")
        allocation.assert_not_called()
        destroy.assert_called_once_with("owned", "a" * 32)
        self.assertIn("readiness", result["errors"][0])

    def test_returned_or_repeated_owner_cannot_replace_the_original_capability(self):
        for index, duplicate in enumerate((False, True)):
            def create(name, *, on_allocated):
                on_allocated("a" * 32)
                if duplicate:
                    on_allocated("b" * 32)
                return "b" * 32
            with patch.object(data, "load_bundle", return_value=MagicMock()), \
                 patch.object(data.runtime, "create_owned", side_effect=create), \
                 patch.object(data.runtime, "Allocation") as allocation, \
                 patch.object(data.runtime, "destroy_owned", return_value={}) as destroy:
                self.cli(self.root / "unused", report=f"duplicate-{index}.json")
            allocation.assert_not_called()
            destroy.assert_called_once_with("owned", "a" * 32)

    def test_rejected_create_or_existing_owner_never_gets_name_only_destroy(self):
        with patch.object(data, "load_bundle", return_value=MagicMock()), \
             patch.object(data.runtime, "create_owned", side_effect=RuntimeError("pre-existing allocation")), \
             patch.object(data.runtime, "destroy_owned") as destroy:
            self.cli(self.root / "unused")
            destroy.assert_not_called()
        with patch.object(data, "load_bundle", return_value=MagicMock()), \
             patch.object(data.runtime, "create_owned") as create, \
             patch.object(data.runtime, "Allocation", side_effect=RuntimeError("drift")), \
             patch.object(data.runtime, "destroy_owned") as destroy:
            result = self.cli(self.root / "unused", owner="a" * 32, report="existing.json")
            create.assert_not_called()
            destroy.assert_not_called()
            self.assertEqual(result["cleanup"]["onebox"], "caller-owned allocation retained")

    def test_constructor_failure_still_destroys_only_original_owned_allocation(self):
        def create(name, *, on_allocated):
            on_allocated("a" * 32)
            return "a" * 32
        with patch.object(data, "load_bundle", return_value=MagicMock()), \
             patch.object(data.runtime, "create_owned", side_effect=create), \
             patch.object(data.runtime, "Allocation", side_effect=RuntimeError("container changed")), \
             patch.object(data.runtime, "destroy_owned", return_value={}) as destroy:
            result = self.cli(self.root / "unused")
            destroy.assert_called_once_with("owned", "a" * 32)
        self.assertIn("container changed", result["errors"][0])

    def test_allocation_reuse_requires_the_exact_original_container_incarnation(self):
        state = {"name": "owned", "owner": "a" * 32, "project": "sf-onebox-owned", "runtime_version": "11.8.121.4"}
        directory = self.root / "build/oneboxes/owned"
        directory.mkdir(parents=True)
        (directory / "state.json").write_text(json.dumps(state))
        container = {
            "Id": "original", "Config": {"Labels": {
                data.runtime.OWNER_LABEL: "a" * 32, "com.docker.compose.project": "sf-onebox-owned",
                "com.docker.compose.service": "onebox"}},
            "State": {"Running": True}, "Mounts": [{"Name": "sf-onebox-owned_data", "Destination": "/var/lib/sf-node"}],
        }
        allocation = data.runtime.Allocation.__new__(data.runtime.Allocation)
        allocation.name, allocation.owner, allocation.container_id = "owned", "a" * 32, "original"
        with patch.object(data.runtime, "ROOT", self.root), patch.object(data.runtime, "run", return_value=json.dumps([container])):
            self.assertEqual(allocation.verify(), "original")
        container["Id"] = "replacement"
        with patch.object(data.runtime, "ROOT", self.root), patch.object(data.runtime, "run", return_value=json.dumps([container])):
            with self.assertRaisesRegex(RuntimeError, "incarnation"):
                allocation.verify()

    def test_destroy_checks_original_container_volume_and_network_independently(self):
        cluster = MagicMock()
        cluster.state = {"owner": "a" * 32}
        cluster.resources.return_value = ([{"Id": "cid"}], [{"Name": "volume"}], [{"Id": "netid"}])
        cluster.destroy.return_value = {"phase": "destroyed"}
        with patch.object(data.runtime.onebox_ctl.ctl, "locked", return_value=contextlib.nullcontext(self.root)), \
             patch.object(data.runtime.onebox_ctl, "Onebox", return_value=cluster), \
             patch.object(data.runtime, "require_resource_absent") as absent:
            result = data.runtime.destroy_owned("owned", "a" * 32)
        self.assertEqual([call.args for call in absent.call_args_list],
                         [("container", "cid"), ("volume", "volume"), ("network", "netid")])
        self.assertTrue(result["absence_verified"])
        cluster.state["owner"] = "b" * 32
        cluster.destroy.reset_mock()
        with patch.object(data.runtime.onebox_ctl.ctl, "locked", return_value=contextlib.nullcontext(self.root)), \
             patch.object(data.runtime.onebox_ctl, "Onebox", return_value=cluster):
            with self.assertRaises(RuntimeError):
                data.runtime.destroy_owned("owned", "a" * 32)
        cluster.destroy.assert_not_called()

    def deployment(self, stage="success", *, visible=True, collision=False, replacement=None, profile="rust"):
        allocation = MagicMock()
        allocation.name, allocation.owner, allocation.container_id = "owned", "a" * 32, "cid"
        allocation.verify.return_value = "cid"
        run_id = "1" * 32
        application, type_name = f"fabric:/FubeData{run_id}", f"FubeData{run_id}Type"
        source = self.root / ("package-" + uuid.uuid4().hex)
        source.mkdir()
        manifest = ET.parse(data.PKG / "rust/ApplicationManifest.xml")
        manifest.getroot().set("ApplicationTypeName", type_name)
        manifest.find(f"{{{data.runtime.NS}}}Parameters/{{{data.runtime.NS}}}Parameter"
                      "[@Name='FubeDataTypeCapability']").set("DefaultValue", "c" * 64)
        manifest.write(source / "ApplicationManifest.xml", encoding="utf-8")
        text = (source / "ApplicationManifest.xml").read_text()
        present = {"app": collision, "type": False, "native_id": type_name + "_App1",
                   "capability": None, "manifest": text, "generation": (131510254310497004, 23, 25769803776)}
        reservation = data.UploadOwnership("/var/lib/sf-node/fube-data-" + run_id,
            "cid", (1, 2), (1, 3), (1, 4), "d" * 64)
        def sf(*args, **kwargs):
            if args[:2] == ("application", "list"):
                return json.dumps({"items": [{"name": application, "typeName": type_name, "typeVersion": "1.1"}]
                                  if present["app"] else []})
            if args[:2] == ("application", "type-list"):
                return json.dumps({"items": [{"name": type_name, "version": "1.1"}] if present["type"] else []})
            if args[:2] == ("application", "provision"):
                present["type"] = visible
                if stage == "provision-rejected":
                    raise RuntimeError("ApplicationTypeAlreadyExists: foreign resource won race")
                if stage in ("provision", "provision-lost-response"):
                    raise subprocess.TimeoutExpired("accepted provision", 1)
            if args[:2] == ("application", "create"):
                present["app"] = visible
                present["capability"] = json.loads(args[args.index("--parameters") + 1])["FubeDataCreationCapability"]
                if stage == "create-rejected":
                    raise RuntimeError("ApplicationAlreadyExists: foreign resource won race")
                if stage in ("create", "create-lost-response"):
                    raise subprocess.TimeoutExpired("accepted create", 1)
            if args[:2] == ("application", "delete"):
                present["app"] = False
            if args[:2] == ("application", "unprovision"):
                present["type"] = False
            return "{}"
        allocation.sf.side_effect = sf
        allocation.execute.side_effect = lambda *args, **kwargs: json.dumps({
            "native_application_id": present["native_id"]})
        def native(_, path):
            if "/GetApplicationManifest?" in path:
                return {"Manifest": present["manifest"]} if present["type"] else None
            if path.startswith("/ImageStore/"):
                if not present["type"] and stage != "generation-collision":
                    return {"StoreFiles": [], "StoreFolders": []}
                if stage == "generation-unavailable":
                    raise RuntimeError("native ImageStore metadata unavailable")
                value = store_generation(data.manifest_identity(text), present["generation"])
                if stage == "generation-missing":
                    value["StoreFiles"][0].pop("FileVersion")
                elif stage == "generation-ambiguous":
                    value["StoreFiles"] *= 2
                return value
            if path.startswith("/Applications/"):
                return {"Id": application.removeprefix("fabric:/"), "Name": application,
                        "TypeName": type_name, "TypeVersion": "1.1", "Status": "Ready", "Parameters": [
                            {"Key": "FubeDataCreationCapability", "Value": present["capability"]}
                        ]} if present["app"] else None
            if path.startswith("/Nodes/"):
                return {"Name": application, "TypeName": type_name,
                        "Status": "Active", "WorkDirectory": "/native/" + present["native_id"] + "/work"
                        } if present["app"] else None
            raise AssertionError(path)
        def run():
            if replacement == "application":
                # Even a replacement with an exact copy of the owner's parameter
                # is not the captured, server-assigned native application ID.
                present["native_id"] = type_name + "_App2"
            elif replacement == "type":
                present["manifest"] = text.replace("c" * 64, "e" * 64)
            elif replacement == "type-generation":
                present["generation"] = (131510254310497004, 24, 25769803776)
            if stage == "scenario-timeout":
                raise subprocess.TimeoutExpired("lost response after original identity captured", 1)
            if stage == "scenario-interrupt":
                raise KeyboardInterrupt
        def wait(check, description, timeout=0):
            value = check()
            if not value:
                raise RuntimeError(description)
            return value
        copy_error = RuntimeError("partial package copy failed")
        copy_error.add_note("original upload reservation: " + json.dumps(reservation.evidence()))
        with patch.object(data.uuid, "uuid4", return_value=MagicMock(hex=run_id)), \
             patch.object(data, "provenance", return_value={}), \
             patch.object(data, "package", return_value=(source, type_name, "1.1")), \
             patch.object(data, "ownership_read", side_effect=native), \
             patch.object(data, "create_data_service", return_value={}), \
             patch.object(data, "reserve_upload", side_effect=(
                 FileExistsError("foreign upload path") if stage == "reserve" else None),
                 return_value=reservation) as reserve, \
             patch.object(data, "check_upload") as upload, \
             patch.object(data.Qualification, "run", side_effect=run), \
             patch.object(data.runtime, "cleanup_deployment", side_effect=AssertionError(
                 "name/attempt-only shared cleanup must not run")), \
             patch.object(data.runtime, "qualify", side_effect=RuntimeError(
                 "modeled preserved legacy runtime failure")) as legacy, \
             patch.object(data.runtime, "wait_condition", side_effect=wait), \
             patch.object(data, "transfer_upload", side_effect=(
                 copy_error if stage == "copy" else None),
                 return_value={"copy_api": "docker cp", "upload_api": "sfctl application upload"}) as copy, \
             patch.object(data.runtime, "failure_artifacts", return_value={"diagnostic": "bounded"}):
            result = data.qualify(allocation, MagicMock(), profile)
        allocation.reservation = reserve
        allocation.upload = upload
        allocation.copy = copy
        allocation.legacy = legacy
        return result, present, allocation

    def test_accepted_timeouts_without_original_native_identity_retain_instead_of_adopting(self):
        for stage in ("provision", "create"):
            result, present, allocation = self.deployment(stage)
            self.assertEqual(result["status"], "FAIL")
            self.assertTrue(present["type"])
            self.assertEqual(present["app"], stage == "create")
            self.assertFalse(any(call.args[:2] in {("application", "delete"), ("application", "unprovision")}
                                 for call in allocation.sf.call_args_list))
            self.assertIn("accepted " + stage, result["errors"][0])
            self.assertEqual(result["upload_paths"], ["/var/lib/sf-node/fube-data-" + result["run_id"]])
            self.assertEqual(result["cleanup"]["upload"], "original reservation removed")
            self.assertIsNone(result["cleanup"]["retained_for_recovery"]["original_native_application_id"])

    def test_definitive_collisions_after_both_absence_checks_never_grant_authority(self):
        for stage in ("provision-rejected", "create-rejected"):
            result, present, allocation = self.deployment(stage)
            self.assertTrue(present["type"])
            self.assertEqual(present["app"], stage == "create-rejected")
            self.assertFalse(any(call.args[:2] in {("application", "delete"), ("application", "unprovision")}
                                 for call in allocation.sf.call_args_list))
            self.assertEqual(result["deployment_outcomes"][stage.split("-")[0]], "rejected")

    def test_successful_original_identity_allows_cleanup_even_after_later_timeout(self):
        for stage in ("success", "scenario-timeout"):
            result, present, allocation = self.deployment(stage)
            self.assertFalse(present["app"])
            self.assertFalse(present["type"])
            self.assertTrue(result["original_application_identity"]["native_application_id"].endswith("_App1"))
            self.assertEqual(result["cleanup"]["application"], "deleted; absence confirmed")
            self.assertEqual(result["cleanup"]["type"], "unprovisioned; absence confirmed")
            self.assertEqual(result["cleanup"]["upload"], "original reservation removed")
            allocation.copy.assert_called_once()
            self.assertEqual(str(allocation.copy.call_args.args[2]), result["local_package_path"])
            self.assertFalse(any(call.args[:2] == ("application", "upload")
                                 for call in allocation.sf.call_args_list))

    def test_interrupted_qualifier_preserves_original_identities_and_cleanup_result(self):
        for profile in ("rust", "mixed"):
            result, present, allocation = self.deployment("scenario-interrupt", profile=profile)
            self.assertEqual(result["status"], "FAIL")
            self.assertIn("KeyboardInterrupt", result["errors"][0])
            self.assertFalse(present["app"])
            self.assertFalse(present["type"])
            self.assertTrue(result["original_application_identity"]["native_application_id"].endswith("_App1"))
            self.assertEqual(result["cleanup"]["application"], "deleted; absence confirmed")
            self.assertEqual(result["cleanup"]["type"], "unprovisioned; absence confirmed")
            self.assertEqual(result["cleanup"]["upload"], "original reservation removed")
            allocation.copy.assert_called_once()

    def test_replacements_with_same_name_type_version_and_copied_token_are_not_original(self):
        for replacement in ("application", "type", "type-generation"):
            result, present, allocation = self.deployment(replacement=replacement)
            self.assertTrue(present["app"])
            self.assertTrue(present["type"])
            self.assertFalse(any(call.args[:2] in {("application", "delete"), ("application", "unprovision")}
                                 for call in allocation.sf.call_args_list))
            self.assertIn("changed", result["cleanup"]["deployment"])
            self.assertTrue(result["original_application_identity"]["native_application_id"].endswith("_App1"))
            self.assertEqual(result["status"], "FAIL")

    def test_missing_ambiguous_or_unavailable_generation_retains_type_and_never_creates_application(self):
        for stage in ("generation-missing", "generation-ambiguous", "generation-unavailable"):
            result, present, allocation = self.deployment(stage)
            self.assertTrue(present["type"])
            self.assertFalse(present["app"])
            self.assertFalse(any(call.args[:2] in {("application", "create"), ("application", "unprovision")}
                                 for call in allocation.sf.call_args_list))
            recovery = result["cleanup"]["retained_for_recovery"]
            self.assertIsNone(recovery["original_native_type_generation"])
            self.assertIn("manifest/generation", result["cleanup"]["type"])
            self.assertEqual(result["cleanup"]["upload"], "original reservation removed")
            self.assertEqual(result["status"], "FAIL")

    def test_existing_provisioned_manifest_path_grants_no_authority_even_if_type_query_reports_absent(self):
        result, _, allocation = self.deployment("generation-collision")
        allocation.reservation.assert_not_called()
        allocation.upload.assert_not_called()
        self.assertFalse(any(call.args[:2] in {("application", "provision"), ("application", "unprovision")}
                             for call in allocation.sf.call_args_list))
        self.assertIn("ImageStore manifest path collision", result["errors"][0])
        self.assertEqual(result["status"], "FAIL")

    def test_lost_mutation_responses_cannot_adopt_foreign_resources(self):
        for stage in ("provision-lost-response", "create-lost-response"):
            result, present, allocation = self.deployment(stage)
            self.assertTrue(present["type"])
            self.assertEqual(present["app"], stage.startswith("create"))
            self.assertFalse(any(call.args[:2] in {("application", "delete"), ("application", "unprovision")}
                                 for call in allocation.sf.call_args_list))
            self.assertIn("retained_for_recovery", result["cleanup"])

    def test_failed_reservation_grants_no_cleanup_but_partial_owned_copy_is_removed(self):
        result, _, allocation = self.deployment("reserve")
        self.assertNotIn("upload_paths", result)
        allocation.upload.assert_not_called()
        allocation.copy.assert_not_called()
        result, _, allocation = self.deployment("copy")
        self.assertIn("partial package copy failed", result["errors"][0])
        self.assertIn(result["upload_reservation"]["capability"],
                      "\n".join(result["qualification_recovery_notes"]))
        self.assertEqual(allocation.upload.call_args.args[2], "remove")
        self.assertEqual(result["cleanup"]["upload"], "original reservation removed")

    def test_ambiguous_absence_remains_failure_and_collision_grants_no_deletion(self):
        result, _, _ = self.deployment("provision", visible=False)
        self.assertTrue(any("late completion possible" in error for error in result["errors"]))
        result, present, allocation = self.deployment("create", collision=True)
        self.assertTrue(present["app"])
        self.assertNotIn("upload_paths", result)
        self.assertFalse(any(call.args[:2] in {("application", "delete"), ("application", "unprovision")}
                             for call in allocation.sf.call_args_list))

    def test_full_records_legacy_runner_failure_without_using_unsafe_shared_cleanup(self):
        result, _, allocation = self.deployment(profile="full")
        self.assertEqual(result["scenarios"]["legacy_full"]["status"], "FAIL")
        self.assertIn("modeled preserved legacy runtime failure", result["scenarios"]["legacy_full"]["error"])
        self.assertTrue(any("legacy_full:" in error for error in result["errors"]))
        allocation.legacy.assert_called_once()
        self.assertIs(allocation.legacy.call_args.args[0], allocation)
        self.assertTrue(allocation.legacy.call_args.kwargs["full"])
        self.assertEqual(allocation.legacy.call_args.kwargs["stateful"], "three")
        self.assertIsInstance(allocation.legacy.call_args.kwargs["deployment_policy"], data.NativeDataDeploymentPolicy)
        self.assertEqual(result["status"], "FAIL")


class LegacyDeploymentTests(ProjectFiles):
    """Run the real legacy dispatcher/config-upgrade body against modeled native I/O."""

    def run_legacy(self, stage="success"):
        bundle = data.load_bundle(self.bundle("bundle-" + uuid.uuid4().hex))
        hashes = {name: data.runtime.digest(bundle.directory / name) for name in data.expected_files()}
        allocation = MagicMock()
        allocation.name, allocation.owner, allocation.container_id = "owned", "a" * 32, "original-cid"
        allocation.verify.return_value = allocation.container_id
        state = {"application": None, "type_name": None, "version": "1.1", "parameters": {},
                 "native_id": None, "types": {}, "uploads": {}, "transfers": {}, "calls": []}

        def reserve(owned, run_id):
            self.assertIs(owned, allocation)
            self.assertRegex(run_id, r"^[a-f0-9]{32}$")
            proof = data.UploadOwnership("/var/lib/sf-node/fube-data-" + run_id,
                allocation.container_id, (1, 2), (1, len(state["uploads"]) + 3),
                (1, len(state["uploads"]) + 30), run_id * 2)
            state["uploads"][proof.path] = proof
            return proof

        def check(owned, proof, action="verify"):
            self.assertIs(owned, allocation)
            self.assertEqual(state["uploads"][proof.path], proof)
            if action == "remove":
                if stage == "v2-upload-drift" and state["transfers"].get(proof.path, (None,))[0] == "1.2":
                    raise RuntimeError("original upload directory incarnation changed; retain")
                del state["uploads"][proof.path]

        def transfer(owned, proof, source):
            self.assertIs(owned, allocation)
            text = (source / "ApplicationManifest.xml").read_text()
            expected = data.manifest_identity(text)
            state["transfers"][proof.path] = expected.version, text
            state["type_name"] = expected.name
            if stage == ("initial-copy" if expected.version == "1.1" else "v2-copy"):
                error = RuntimeError("partial package copy")
                error.add_note("original upload reservation: " + json.dumps(proof.evidence()))
                raise error
            return {"copy_api": "docker cp", "upload_api": "sfctl application upload"}

        def sf(*args, **kwargs):
            state["calls"].append(args)
            if args[:2] == ("application", "list"):
                return json.dumps({"items": [{"name": state["application"]}] if state["application"] else []})
            if args[:2] == ("application", "type-list"):
                return json.dumps({"items": [{"name": state["type_name"], "version": version}
                                             for version in state["types"]]})
            if args[:2] == ("application", "provision"):
                path = "/var/lib/sf-node/" + args[args.index("--application-type-build-path") + 1]
                version, text = state["transfers"][path]
                state["types"][version] = {"text": text, "generation": (131510254310497004,
                    23 if version == "1.1" else 24, 25769803776)}
                prefix = "initial" if version == "1.1" else "v2"
                if stage == prefix + "-provision-timeout":
                    raise subprocess.TimeoutExpired("accepted " + prefix + " provision", 1)
                if stage == prefix + "-provision-rejected":
                    raise RuntimeError("ApplicationTypeAlreadyExists")
            elif args[:2] == ("application", "create"):
                state["application"] = args[args.index("--app-name") + 1]
                state["parameters"] = json.loads(args[args.index("--parameters") + 1])
                state["native_id"] = state["type_name"] + "_App1"
                if stage == "initial-create-timeout":
                    raise subprocess.TimeoutExpired("accepted create", 1)
                if stage == "initial-create-rejected":
                    raise RuntimeError("ApplicationAlreadyExists")
            elif args[:2] == ("application", "upgrade"):
                self.assertEqual(json.loads(args[args.index("--parameters") + 1]), state["parameters"])
                self.assertEqual(args[-2:], ("--mode", "UnmonitoredAuto"))
                state["version"] = args[args.index("--application-version") + 1]
                if stage == "upgrade-timeout":
                    raise subprocess.TimeoutExpired("accepted native upgrade", 1)
            elif args[:2] == ("application", "upgrade-status"):
                return '{"upgradeState":"RollingForwardCompleted"}'
            elif args[:2] == ("application", "delete"):
                state["application"] = None
            elif args[:2] == ("application", "unprovision"):
                del state["types"][args[-1]]
            else:
                self.assertEqual(args[:2], ("cluster", "select"), "package upload is modeled by transfer_upload")
            return "{}"
        allocation.sf.side_effect = sf

        def native(owned, path):
            self.assertIs(owned, allocation)
            if path.startswith("/ImageStore/"):
                version = "1.2" if ".1.2.xml" in path else "1.1"
                record = state["types"].get(version)
                if record is None:
                    return {"StoreFiles": [], "StoreFolders": []}
                if stage == ("initial-generation-unavailable" if version == "1.1" else "v2-generation-unavailable"):
                    raise RuntimeError("native FileVersion unavailable")
                return store_generation(data.manifest_identity(record["text"]), record["generation"])
            if "/GetApplicationManifest?" in path:
                version = "1.2" if "ApplicationTypeVersion=1.2" in path else "1.1"
                record = state["types"].get(version)
                return {"Manifest": record["text"]} if record else None
            if path.startswith("/Applications/"):
                return {"Name": state["application"], "TypeName": state["type_name"],
                        "TypeVersion": state["version"], "Status": "Ready",
                        "Parameters": [{"Key": key, "Value": value} for key, value in state["parameters"].items()]
                        } if state["application"] else None
            if path.startswith("/Nodes/"):
                return {"Name": state["application"], "TypeName": state["type_name"], "Status": "Active",
                        "WorkDirectory": "/native/" + state["native_id"] + "/work"} if state["application"] else None
            self.fail(path)

        def execute(*args, **kwargs):
            if args[2] == data.OWNERSHIP_READ:
                self.assertRegex(args[3],r"^/ImageStore/fube-data-[a-f0-9]{32}\?api-version=6\.2$")
                return json.dumps({"status":404,"body":None})
            if args[2] == data.APPLICATION_PACKAGE_ID:
                if stage == "initial-native-id-unavailable":
                    raise RuntimeError("actual native ApplicationId unavailable")
                return json.dumps({"native_application_id": state["native_id"]})
            self.assertEqual(args[2], data.runtime.PROCESS)
            return json.dumps({"start_ticks": 10, "executable_sha256": data.runtime.digest(bundle.legacy.app)})
        allocation.execute.side_effect = execute

        def observe(*args, **kwargs):
            self.assertEqual(kwargs["observer"], bundle.legacy.observer)
            after = state["version"] == "1.2"
            before_config = {"value": "before-upgrade", "path": "/native/config-v1"}
            current = {"value": "after-upgrade", "path": "/native/config-v2"} if after else before_config
            generation = 2 if after else 1
            return {"native_entry_point_instance": 7, "native_code_version": "1.1",
                    "rpc": {"partition": "partition", "instance": 1, "incarnation": 2, "pid": 3, "host": "host",
                            "packages": {"configuration": current, "generation": generation, "runtime_id": "original",
                                "initial_configuration": before_config, "retained_configuration": before_config,
                                "unregistered_callbacks": 0, "events": [
                                    {"registration": index, "previous": before_config, "current": current,
                                     "published_generation": generation} for index in (1, 2)] if after else []}}}

        def wait(check, description, timeout=0):
            value = check()
            if not value:
                raise RuntimeError(description)
            return value

        dispatched = []
        def scenario(name):
            def invoke(*args, **kwargs):
                dispatched.append(name)
                if name == "restart":
                    if stage == "application-replacement":
                        state["native_id"] = state["type_name"] + "_App2"
                    elif stage == "version-drift":
                        state["version"] = "9.9"
                    elif stage.startswith("generation-drift-"):
                        version = stage.removeprefix("generation-drift-")
                        state["types"][version]["generation"] = (131510254310497004, 99, 25769803776)
                if name == "controls_and_authority":
                    self.assertEqual(kwargs["expected_generation"], 2)
                return {}
            return invoke
        outer = {"scenarios": {"legacy_full": {"status": "MISSING", "reason": "not executed"}}, "errors": []}
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(data.runtime, "ROOT", self.root))
            stack.enter_context(patch.object(data, "reserve_upload", side_effect=reserve))
            stack.enter_context(patch.object(data, "check_upload", side_effect=check))
            stack.enter_context(patch.object(data, "transfer_upload", side_effect=transfer))
            stack.enter_context(patch.object(data, "ownership_read", side_effect=native))
            stack.enter_context(patch.object(data.runtime, "wait_condition", side_effect=wait))
            stack.enter_context(patch.object(data.runtime, "wait_observe", side_effect=observe))
            stack.enter_context(patch.object(data.runtime, "observe", side_effect=observe))
            stack.enter_context(patch.object(data.runtime, "deployment_artifacts", return_value={
                "code_rollout": "1", "code_checksum": "unchanged"}))
            stack.enter_context(patch.object(data.runtime, "failure_artifacts", return_value={}))
            for name in ("stateful_singleton", "stateful_three", "controls_and_authority", "restart", "wait_stateful"):
                stack.enter_context(patch.object(data.runtime, name, side_effect=scenario(name)))
            unsafe = stack.enter_context(patch.object(data.runtime, "cleanup_deployment",
                side_effect=AssertionError("unsafe legacy cleanup")))
            allocation_create = stack.enter_context(patch.object(data.runtime, "create_owned",
                side_effect=AssertionError("must reuse the original allocation")))
            allocation_destroy = stack.enter_context(patch.object(data.runtime, "destroy_owned",
                side_effect=AssertionError("the outer data runner owns allocation cleanup")))
            data.legacy_full(allocation, bundle, outer)
            unsafe.assert_not_called()
            allocation_create.assert_not_called()
            allocation_destroy.assert_not_called()
        self.assertEqual(hashes, {name: data.runtime.digest(bundle.directory / name) for name in data.expected_files()},
                         "minted owner capabilities must never mutate stored templates")
        return outer, state, dispatched

    def test_real_full_dispatch_and_configuration_upgrade_preserve_original_identity_and_clean_both_versions(self):
        outer, state, dispatched = self.run_legacy()
        evidence = outer["scenarios"]["legacy_full"]
        self.assertEqual(evidence["status"], "PASS", evidence)
        nested = evidence["qualification"]
        self.assertEqual(dispatched, ["stateful_singleton", "stateful_three", "controls_and_authority",
                                      "restart", "wait_stateful"])
        self.assertEqual(nested["configuration_upgrade"]["status"], "PASS_UNCHANGED_CODE_NATIVE_CONTEXT_REFRESH")
        self.assertEqual(nested["configuration_upgrade"]["native_application_identity"]["native_application_id"],
                         nested["original_application_identity"]["native_application_id"])
        self.assertEqual(nested["configuration_upgrade"]["native_upgrade_status"]["upgradeState"],
                         "RollingForwardCompleted")
        self.assertEqual(len([call for call in state["calls"] if call[:2] == ("application", "upgrade")]), 1)
        self.assertIsNone(state["application"])
        self.assertEqual(state["types"], {})
        self.assertEqual(state["uploads"], {})
        versions = nested["deployment_versions"]
        self.assertEqual(set(versions), {"1.1", "1.2"})
        self.assertNotEqual(versions["1.1"]["original_type"]["native_generation"],
                            versions["1.2"]["original_type"]["native_generation"])
        for version in versions.values():
            manifest = ET.parse(Path(version["local_package_path"]) / "ApplicationManifest.xml")
            parameters = {item.attrib["Name"]: item.attrib["DefaultValue"] for item in
                          manifest.findall(f"{{{data.runtime.NS}}}Parameters/{{{data.runtime.NS}}}Parameter")}
            self.assertEqual(parameters, state["parameters"])
        self.assertEqual(nested["cleanup"]["independent_absence"], {
            "application": True, "types": {"1.1": True, "1.2": True}, "uploads": {"1.1": True, "1.2": True}})
        self.assertTrue(nested["cleanup"]["safe_owner_cleanup"])

    def test_partial_initial_mutations_never_adopt_unknown_startup_or_rejected_instances(self):
        for stage in ("initial-provision-timeout", "initial-provision-rejected",
                      "initial-create-timeout", "initial-create-rejected", "initial-native-id-unavailable",
                      "initial-generation-unavailable"):
            with self.subTest(stage=stage):
                outer, state, _ = self.run_legacy(stage)
                self.assertEqual(outer["scenarios"]["legacy_full"]["status"], "FAIL")
                nested = outer["scenarios"]["legacy_full"]["qualification"]
                self.assertTrue(state["types"])
                self.assertFalse(any(call[:2] in {("application", "delete"), ("application", "unprovision")}
                                     for call in state["calls"]))
                self.assertEqual(state["uploads"], {})
                self.assertIn("retained_for_recovery", nested["cleanup"])
                self.assertIsNone(nested["cleanup"]["retained_for_recovery"]["original_native_application_id"])

    def test_partial_copies_clean_only_the_original_reservations_and_known_initial_deployment(self):
        for stage in ("initial-copy", "v2-copy", "v2-provision-timeout",
                      "v2-provision-rejected", "v2-generation-unavailable"):
            with self.subTest(stage=stage):
                outer, state, _ = self.run_legacy(stage)
                nested = outer["scenarios"]["legacy_full"]["qualification"]
                self.assertEqual(outer["scenarios"]["legacy_full"]["status"], "FAIL")
                self.assertIsNone(state["application"])
                self.assertNotIn("1.1", state["types"])
                self.assertEqual(state["uploads"], {})
                if "provision" in stage or "generation" in stage:
                    self.assertIn("1.2", state["types"])
                    recovery = nested["cleanup"]["retained_for_recovery"]
                    self.assertIsNone(recovery["versions"]["1.2"]["original_type"])
                    self.assertIn("no captured original manifest/generation", recovery["reasons"]["type 1.2"])
                else:
                    self.assertEqual(state["types"], {})
                    self.assertIn("original upload reservation", "\n".join(nested["qualification_recovery_notes"]))

    def test_native_upgrade_timeout_is_failure_even_when_same_original_instance_is_safely_cleaned(self):
        outer, state, dispatched = self.run_legacy("upgrade-timeout")
        nested = outer["scenarios"]["legacy_full"]["qualification"]
        self.assertEqual(outer["scenarios"]["legacy_full"]["status"], "FAIL")
        self.assertTrue(nested["cleanup"]["safe_owner_cleanup"])
        self.assertIn("accepted native upgrade", nested["errors"][0])
        self.assertNotIn("stateful_three", dispatched)
        self.assertIsNone(state["application"])
        self.assertEqual(state["types"], {})
        self.assertEqual(state["uploads"], {})

    def test_version_generation_and_copied_capability_drift_retain_deployment_but_cleanup_uploads(self):
        for stage in ("version-drift", "application-replacement", "generation-drift-1.1", "generation-drift-1.2"):
            with self.subTest(stage=stage):
                outer, state, _ = self.run_legacy(stage)
                nested = outer["scenarios"]["legacy_full"]["qualification"]
                self.assertEqual(outer["scenarios"]["legacy_full"]["status"], "FAIL")
                self.assertIsNotNone(state["application"])
                self.assertEqual(set(state["types"]), {"1.1", "1.2"})
                self.assertEqual(state["uploads"], {})
                self.assertIn("deployment", nested["cleanup"]["retained"])
                self.assertFalse(any(call[:2] in {("application", "delete"), ("application", "unprovision")}
                                     for call in state["calls"]))

    def test_upload_replacement_fails_cleanup_gate_even_after_real_legacy_native_checks_pass(self):
        outer, state, _ = self.run_legacy("v2-upload-drift")
        nested = outer["scenarios"]["legacy_full"]["qualification"]
        self.assertEqual(outer["scenarios"]["legacy_full"]["status"], "FAIL")
        self.assertIsNone(state["application"])
        self.assertEqual(state["types"], {})
        self.assertEqual(len(state["uploads"]), 1)
        self.assertIn("upload 1.2", nested["cleanup"]["retained"])
        self.assertFalse(nested["cleanup"]["safe_owner_cleanup"])

    def test_fabricated_expected_status_without_running_policy_cannot_pass_full_scenario(self):
        bundle = data.load_bundle(self.bundle())
        result = {"scenarios": {"legacy_full": {"status": "MISSING"}}, "errors": []}
        with patch.object(data.runtime, "qualify", return_value={
            "status": "PASS_FULL_EMPTY_STATEFUL_CONTROL_PLANE_AND_STATELESS_PROFILE", "errors": [],
            "cleanup": {"safe_owner_cleanup": True}}):
            data.legacy_full(MagicMock(), bundle, result)
        self.assertEqual(result["scenarios"]["legacy_full"]["status"], "FAIL")
        self.assertIn("original-owner cleanup", result["scenarios"]["legacy_full"]["error"])


class UploadTests(ProjectFiles):
    def setUp(self):
        super().setUp()
        parent = patch.object(data.staging, "PARENT", self.root)
        parent.start()
        self.addCleanup(parent.stop)
        self.source = self.root / "source"
        (self.source / "Code").mkdir(parents=True)
        (self.source / "ApplicationManifest.xml").write_bytes(b"manifest")
        (self.source / "ApplicationManifest.xml").chmod(0o644)
        (self.source / "Code/app").write_bytes(b"executable")
        (self.source / "Code/app").chmod(0o755)
        self.allocation = MagicMock()
        self.allocation.verify.return_value = "original-cid"
        self.allocation.execute.side_effect = self.execute
        self.uploads = []
        self.on_upload = None
        self.proof = data.reserve_upload(self.allocation, "1" * 32)
        self.path = Path(self.proof.path)
        self.remote = self.path / self.path.name
        copier = patch.object(data.runtime, "run", side_effect=self.copy)
        self.copier = copier.start()
        self.addCleanup(copier.stop)

    def execute(self, *args, **kwargs):
        if args[:3] == ("python3", "-c", data.STAGING_PROGRAM):
            with patch.object(sys, "argv", ["staging", *args[3:]]), \
                 contextlib.redirect_stdout(io.StringIO()) as output:
                data.staging.main()
            return output.getvalue()
        self.assertEqual(args[:3], ("timeout", "--signal=TERM", "--kill-after=5s"))
        self.assertEqual(args[4:7], ("sfctl", "application", "upload"))
        self.assertEqual(args[7:9], ("--path", str(self.remote)))
        self.assertEqual(args[9], "--timeout")
        self.assertGreater(int(args[10]), 0)
        self.assertLessEqual(int(args[10]), data.UPLOAD_SECONDS)
        self.assertGreater(float(args[3].removesuffix("s")), 0)
        self.assertLessEqual(float(args[3].removesuffix("s")), data.UPLOAD_SECONDS)
        self.assertLessEqual(kwargs["timeout"], data.UPLOAD_SECONDS + 10)
        self.uploads.append(data.staging.inventory(self.remote))
        if self.on_upload is not None:
            self.on_upload()
        return ""

    def copy(self, *args, **kwargs):
        self.assertEqual(args, ("docker", "cp", str(self.source), f"original-cid:{self.remote}"))
        self.assertLessEqual(kwargs["timeout"], data.UPLOAD_SECONDS)
        shutil.copytree(self.source, self.remote)
        return ""

    def test_sfctl_uploads_only_exact_package_and_owned_staging_is_removed(self):
        result = data.transfer_upload(self.allocation, self.proof, self.source)
        expected = {
            "ApplicationManifest.xml": {"kind": "file", "mode": 0o644, "bytes": 8,
                                       "sha256": hashlib.sha256(b"manifest").hexdigest()},
            "Code": {"kind": "directory", "mode": (self.source / "Code").stat().st_mode & 0o777},
            "Code/app": {"kind": "file", "mode": 0o755, "bytes": 10,
                         "sha256": hashlib.sha256(b"executable").hexdigest()},
        }
        self.assertEqual(self.uploads, [expected])
        self.assertEqual(result["inventory"], expected)
        self.assertEqual(result["image_store_path"], self.path.name)
        self.assertEqual(result["container_id"], "original-cid")
        self.assertEqual(result["upload_api"], "sfctl application upload")
        self.assertEqual(result["copy_api"], "docker cp")
        self.assertFalse((self.remote / data.staging.OWNER).exists())
        self.assertTrue((self.path / data.staging.OWNER).is_file())
        self.copier.assert_called_once()
        data.check_upload(self.allocation, self.proof, "remove")
        self.assertFalse(self.path.exists())

    def test_staging_program_runs_unchanged_in_real_child(self):
        path = self.root / ("fube-data-" + "2" * 32)
        proof = {"capability": "a" * 64}
        for action in ("reserve", "verify", "remove"):
            result = subprocess.run([sys.executable, "-c", data.STAGING_PROGRAM, str(self.root),
                str(path), action, json.dumps(proof)], capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            captured = json.loads(result.stdout)
            if action != "reserve":
                self.assertEqual(captured, proof)
            proof = captured
        self.assertFalse(path.exists())

    def test_existing_paths_are_not_adopted_or_modified(self):
        for index, kind in enumerate(("directory", "file", "symlink"), 2):
            with self.subTest(kind=kind):
                run_id = f"{index:032x}"
                path = self.root / ("fube-data-" + run_id)
                if kind == "directory":
                    path.mkdir()
                    (path / "sentinel").write_text("foreign")
                elif kind == "file":
                    path.write_text("foreign")
                else:
                    path.symlink_to(self.source)
                with self.assertRaises(FileExistsError):
                    data.reserve_upload(self.allocation, run_id)
                if kind == "directory":
                    self.assertEqual((path / "sentinel").read_text(), "foreign")
                    self.assertFalse((path / data.staging.OWNER).exists())
                elif kind == "file":
                    self.assertEqual(path.read_text(), "foreign")
                else:
                    self.assertEqual(path.resolve(), self.source)

    def test_same_name_replacement_with_copied_capability_is_retained(self):
        original = self.root / "original"
        self.path.rename(original)
        self.path.mkdir()
        shutil.copyfile(original / data.staging.OWNER, self.path / data.staging.OWNER)
        (self.path / "sentinel").write_text("foreign")
        with self.assertRaisesRegex(RuntimeError, "capability changed|incarnation changed"):
            data.check_upload(self.allocation, self.proof, "remove")
        self.assertEqual((self.path / "sentinel").read_text(), "foreign")
        self.assertTrue(original.is_dir())

    def test_replaced_owner_file_does_not_authorize_cleanup(self):
        owner = self.path / data.staging.OWNER
        owner.rename(self.path / "original-owner")
        owner.write_text(self.proof.capability)
        with self.assertRaisesRegex(RuntimeError, "capability changed"):
            data.check_upload(self.allocation, self.proof, "remove")
        self.assertTrue(self.path.is_dir())

    def test_invalid_packages_fail_before_copy_or_upload(self):
        for kind in ("symlink", "fifo", "mode", "reserved"):
            with self.subTest(kind=kind):
                path = self.source / ("_.dir" if kind == "reserved" else "invalid")
                if kind == "symlink":
                    path.symlink_to(self.source / "Code/app")
                elif kind == "fifo":
                    os.mkfifo(path)
                else:
                    path.write_bytes(b"invalid")
                    path.chmod(0o664 if kind == "mode" else 0o644)
                with self.assertRaisesRegex(RuntimeError, "regular files|invalid package path"):
                    data.transfer_upload(self.allocation, self.proof, self.source)
                path.unlink()
        for limit in ("MAX_ENTRIES", "MAX_BYTES"):
            with self.subTest(limit=limit), patch.object(data.staging, limit, 1):
                with self.assertRaisesRegex(RuntimeError, "package (entry|byte) bound"):
                    data.transfer_upload(self.allocation, self.proof, self.source)
        self.copier.assert_not_called()
        self.assertEqual(self.uploads, [])

    def test_partial_copy_failure_preserves_original_cleanup_and_error(self):
        error = subprocess.TimeoutExpired("docker cp", 1)
        def partial(*args, **kwargs):
            self.copy(*args, **kwargs)
            raise error
        self.copier.side_effect = partial
        with self.assertRaises(subprocess.TimeoutExpired) as raised:
            data.transfer_upload(self.allocation, self.proof, self.source)
        self.assertIs(raised.exception, error)
        self.assertIn(self.proof.capability, "\n".join(error.__notes__))
        self.assertEqual(self.uploads, [])
        data.check_upload(self.allocation, self.proof, "remove")
        self.assertFalse(self.path.exists())

    def test_sfctl_failure_preserves_original_cleanup_and_error(self):
        error = RuntimeError("sfctl upload failed")
        def fail():
            raise error
        self.on_upload = fail
        with self.assertRaisesRegex(RuntimeError, "sfctl upload failed") as raised:
            data.transfer_upload(self.allocation, self.proof, self.source)
        self.assertIs(raised.exception, error)
        self.assertIn(self.proof.capability, "\n".join(error.__notes__))
        self.assertTrue(self.remote.is_dir())
        data.check_upload(self.allocation, self.proof, "remove")
        self.assertFalse(self.path.exists())

    def test_allocation_drift_prevents_copy_or_upload_into_replacement(self):
        self.allocation.verify.return_value = "replacement-cid"
        with self.assertRaisesRegex(RuntimeError, "allocation changed"):
            data.transfer_upload(self.allocation, self.proof, self.source)
        self.copier.assert_not_called()
        self.assertEqual(self.uploads, [])
        self.allocation.verify.side_effect = ["original-cid", "replacement-cid"]
        with self.assertRaisesRegex(RuntimeError, "allocation changed"):
            data.transfer_upload(self.allocation, self.proof, self.source)
        self.assertEqual(self.uploads, [])

    def test_changed_staged_hash_prevents_sfctl_upload(self):
        def changed(*args, **kwargs):
            self.copy(*args, **kwargs)
            (self.remote / "Code/app").write_bytes(b"wrong executable")
        self.copier.side_effect = changed
        with self.assertRaisesRegex(RuntimeError, "copied package inventory/hash changed"):
            data.transfer_upload(self.allocation, self.proof, self.source)
        self.assertEqual(self.uploads, [])

    def test_local_or_staged_drift_during_upload_cannot_report_success(self):
        for target in (self.source, self.remote):
            with self.subTest(target=target):
                self.on_upload = lambda: (target / "Code/app").write_bytes(b"changed")
                with self.assertRaisesRegex(RuntimeError, "uploaded package inventory/hash changed"):
                    data.transfer_upload(self.allocation, self.proof, self.source)
                shutil.rmtree(self.remote)
                (self.source / "Code/app").write_bytes(b"executable")

    def test_expired_deadline_never_starts_copy_or_upload(self):
        with patch.object(data.time, "monotonic", side_effect=[0, data.UPLOAD_SECONDS + 1]):
            with self.assertRaisesRegex(RuntimeError, "upload deadline expired"):
                data.transfer_upload(self.allocation, self.proof, self.source)
        self.copier.assert_not_called()
        self.assertEqual(self.uploads, [])

    def test_fractional_deadline_never_disables_container_timeout_by_rounding_to_zero(self):
        clock = [0, .1, .2, 179.99999, 179.999991, 179.999992]
        with patch.object(data.time, "monotonic", side_effect=clock):
            data.transfer_upload(self.allocation, self.proof, self.source)
        self.assertEqual(len(self.uploads), 1)


class NativeOwnershipIdentityTests(ProjectFiles):
    def test_application_instance_id_comes_from_native_package_not_the_rest_encoded_name(self):
        native_id = "MintedType_App412"
        base = self.root / native_id
        work = base / "work"
        work.mkdir(parents=True)
        manifest = base / "App.1.0.xml"
        manifest.write_text(f'<ApplicationPackage xmlns="{data.runtime.NS}" '
                            f'ApplicationId="{native_id}" ApplicationTypeName="MintedType" '
                            'NameUri="fabric:/Owned" RolloutVersion="1.0"/>')
        def run():
            with patch("sys.argv", ["native", str(work), "fabric:/Owned", "MintedType"]), \
                 contextlib.redirect_stdout(io.StringIO()) as output:
                exec(data.APPLICATION_PACKAGE_ID, {})
            return json.loads(output.getvalue())
        self.assertEqual(run(), {"native_application_id": native_id})
        manifest.write_text(manifest.read_text().replace(native_id, "OtherType_App413"))
        with self.assertRaisesRegex(RuntimeError, "instance ID/path mismatch"):
            run()

    def test_type_manifest_fingerprint_binds_content_version_and_run_creation_marker(self):
        text = (data.PKG / "rust/ApplicationManifest.xml").read_text().replace(
            'Name="FubeDataTypeCapability" DefaultValue=""',
            'Name="FubeDataTypeCapability" DefaultValue="' + "a" * 64 + '"')
        original = data.replace(data.manifest_identity(text), native_generation=(131510254310497004, 23, 25769803776))
        allocation = MagicMock()
        for replacement in (text.replace("a" * 64, "b" * 64),
                            text.replace('ApplicationTypeVersion="1.1"', 'ApplicationTypeVersion="1.2"'),
                            text.replace("native-override", "foreign-override")):
            with patch.object(data, "ownership_read", return_value={"Manifest": replacement}):
                with self.assertRaisesRegex(RuntimeError, "manifest/capability changed"):
                    data.native_type_identity(allocation, original)
        with patch.object(data, "ownership_read", side_effect=lambda _, path:
                          store_generation(original) if path.startswith("/ImageStore/") else
                          {"Manifest": text.replace("  <", "    <") + "\n"}):
            self.assertEqual(data.native_type_identity(allocation, original), original)

    def test_original_type_can_be_cleaned_without_an_application_but_replaced_manifest_is_retained(self):
        text = (data.PKG / "rust/ApplicationManifest.xml").read_text().replace(
            'Name="FubeDataTypeCapability" DefaultValue=""',
            'Name="FubeDataTypeCapability" DefaultValue="' + "a" * 64 + '"')
        requested = data.manifest_identity(text)
        for replacement in (False, True):
            result = {"application": "fabric:/Owned", "type_name": requested.name, "version": requested.version,
                      "errors": [], "cleanup": {}}
            allocation = MagicMock()
            present = {"manifest": text}
            def sf(*args):
                self.assertEqual(args[:2], ("application", "unprovision"))
                present["manifest"] = None
            allocation.sf.side_effect = sf
            with patch.object(data, "ownership_read", side_effect=lambda _, path:
                              store_generation(requested) if path.startswith("/ImageStore/") else
                              {"Manifest": present["manifest"]} if present["manifest"] else None):
                original = data.native_type_identity(allocation, requested, capture=True)
                if replacement:
                    present["manifest"] = text.replace("native-override", "foreign-override")
                data.cleanup_data_deployment(allocation, result,
                    {"provision": "accepted", "create": "not_requested"}, original, None, None)
            if replacement:
                allocation.sf.assert_not_called()
                self.assertIn("changed", result["cleanup"]["deployment"])
            else:
                allocation.sf.assert_called_once()
                self.assertEqual(result["cleanup"]["type"], "unprovisioned; absence confirmed")

    def type_manifest(self):
        return (data.PKG / "rust/ApplicationManifest.xml").read_text().replace(
            'Name="FubeDataTypeCapability" DefaultValue=""',
            'Name="FubeDataTypeCapability" DefaultValue="' + "a" * 64 + '"')

    def test_identical_manifest_reprovision_with_different_native_generation_is_never_unprovisioned(self):
        text = self.type_manifest()
        requested = data.manifest_identity(text)
        for field in range(3):
            with self.subTest(field=field):
                allocation = MagicMock()
                generation = [131510254310497004, 23, 25769803776]
                result = {"application": "fabric:/Owned", "type_name": requested.name,
                          "version": requested.version, "errors": [], "cleanup": {}}
                def native(_, path):
                    return (store_generation(requested, generation) if path.startswith("/ImageStore/")
                            else {"Manifest": text})
                with patch.object(data, "ownership_read", side_effect=native):
                    original = data.native_type_identity(allocation, requested, capture=True)
                    generation[field] += 1
                    data.cleanup_data_deployment(allocation, result,
                        {"provision": "accepted", "create": "not_requested"}, original, None, None)
                allocation.sf.assert_not_called()
                self.assertIn("generation changed", result["cleanup"]["deployment"])
                self.assertTrue(result["errors"])
                self.assertEqual(result["cleanup"]["retained_for_recovery"]["original_native_type_generation"],
                                 original.native_generation)

    def test_replacement_during_native_generation_capture_is_not_pinned_as_original(self):
        text = self.type_manifest()
        requested = data.manifest_identity(text)
        generation_reads = []
        def native(_, path):
            if not path.startswith("/ImageStore/"):
                return {"Manifest": text}
            generation_reads.append(path)
            return store_generation(requested, (131510254310497004, 22 + len(generation_reads), 25769803776))
        with patch.object(data, "ownership_read", side_effect=native):
            with self.assertRaisesRegex(RuntimeError, "generation changed during capture"):
                data.native_type_identity(MagicMock(), requested, capture=True)
        self.assertEqual(len(generation_reads), 2)

    def test_replacement_between_initial_manifest_and_generation_queries_cannot_be_adopted(self):
        text = self.type_manifest()
        requested = data.manifest_identity(text)
        generation = [131510254310497004, 23, 25769803776]
        manifests = []
        def native(_, path):
            if path.startswith("/ImageStore/"):
                return store_generation(requested, generation)
            if not manifests:
                # The review's identical package, including copied marker,
                # but a different server-assigned provisioning file lifetime.
                generation[1] += 1
            manifests.append(path)
            return {"Manifest": text}
        with patch.object(data, "ownership_read", side_effect=native):
            with self.assertRaisesRegex(RuntimeError, "generation changed during capture"):
                data.native_type_identity(MagicMock(), requested, capture=True)

    def test_missing_ambiguous_malformed_and_staging_generations_grant_no_type_authority(self):
        text = self.type_manifest()
        requested = data.manifest_identity(text)
        valid = store_generation(requested)
        invalid = [None, {}, {"StoreFiles": []}, {**valid, "StoreFiles": valid["StoreFiles"] * 2},
                   {**valid, "ContinuationToken": "more"}]
        for field, value in (("FileVersion", None), ("StoreRelativePath", "fube-data-upload/ApplicationManifest.xml")):
            item = copy.deepcopy(valid)
            item["StoreFiles"][0][field] = value
            invalid.append(item)
        for version in ({"EpochDataLossNumber": "1", "VersionNumber": "2"},
                        {"EpochDataLossNumber": "1", "VersionNumber": True, "EpochConfigurationNumber": "3"},
                        {"EpochDataLossNumber": "1", "VersionNumber": str(2**64), "EpochConfigurationNumber": "3"}):
            item = copy.deepcopy(valid)
            item["StoreFiles"][0]["FileVersion"] = version
            invalid.append(item)
        for value in invalid:
            with self.subTest(value=value), patch.object(data, "ownership_read", side_effect=lambda _, path:
                            value if path.startswith("/ImageStore/") else {"Manifest": text}):
                with self.assertRaisesRegex(RuntimeError, "generation|FileVersion"):
                    data.native_type_identity(MagicMock(), requested, capture=True)

    def test_loss_of_generation_metadata_at_teardown_retains_the_original_type(self):
        text = self.type_manifest()
        requested = data.manifest_identity(text)
        valid = store_generation(requested)
        for unavailable in ({"StoreFiles": []}, {**valid, "StoreFiles": valid["StoreFiles"] * 2}):
            with self.subTest(unavailable=unavailable):
                current = valid
                allocation = MagicMock()
                result = {"application": "fabric:/Owned", "type_name": requested.name,
                          "version": requested.version, "errors": [], "cleanup": {}}
                with patch.object(data, "ownership_read", side_effect=lambda _, path:
                                  current if path.startswith("/ImageStore/") else {"Manifest": text}):
                    original = data.native_type_identity(allocation, requested, capture=True)
                    current = unavailable
                    data.cleanup_data_deployment(allocation, result,
                        {"provision": "accepted", "create": "not_requested"}, original, None, None)
                allocation.sf.assert_not_called()
                self.assertIn("generation unavailable/ambiguous", result["cleanup"]["deployment"])
                self.assertEqual(result["cleanup"]["retained_for_recovery"]["original_native_type_generation"],
                                 original.native_generation)

    def test_content_only_identity_cannot_be_promoted_into_cleanup_authority(self):
        requested = data.manifest_identity(self.type_manifest())
        allocation = MagicMock()
        result = {"application": "fabric:/Owned", "type_name": requested.name, "version": requested.version,
                  "errors": [], "cleanup": {}}
        with patch.object(data, "ownership_read") as read:
            with self.assertRaisesRegex(RuntimeError, "manifest-only adoption"):
                data.native_type_identity(allocation, requested)
            data.cleanup_data_deployment(allocation, result,
                {"provision": "accepted", "create": "not_requested"}, requested, None, None)
        read.assert_not_called()
        allocation.sf.assert_not_called()
        self.assertIn("refuse manifest-only cleanup", result["cleanup"]["deployment"])

    def test_explicit_rejection_revokes_authority_even_if_a_matching_identity_was_supplied(self):
        original_type = data.TypeIdentity("Type", "1.1", "a" * 64, "b" * 64)
        original_app = data.ApplicationIdentity("fabric:/Owned", original_type, "c" * 64, "Type_App1")
        for stage in ("provision", "create"):
            result = {"application": "fabric:/Owned", "type_name": "Type", "version": "1.1",
                      "errors": [], "cleanup": {}}
            allocation = MagicMock()
            outcomes = {"provision": "accepted", "create": "accepted"}
            outcomes[stage] = "rejected"
            with patch.object(data, "ownership_read") as read:
                data.cleanup_data_deployment(allocation, result, outcomes,
                                             original_type, original_app, None)
            allocation.sf.assert_not_called()
            read.assert_not_called()
            self.assertIn("cannot grant original", result["cleanup"]["deployment"])

    def test_missing_native_instance_cannot_be_substituted_by_a_copied_application_parameter(self):
        expected = data.TypeIdentity("Type", "1.1", "a" * 64, "b" * 64)
        allocation = MagicMock()
        app = {"Id": "Owned", "Name": "fabric:/Owned", "TypeName": "Type", "TypeVersion": "1.1", "Status": "Ready",
               "Parameters": [{"Key": "FubeDataCreationCapability", "Value": "c" * 64}]}
        with patch.object(data, "ownership_read", side_effect=lambda _, path:
                          None if path.startswith("/Nodes/") else app), \
             patch.object(data, "native_type_identity", return_value=expected):
            with self.assertRaisesRegex(RuntimeError, "unavailable/ambiguous"):
                data.native_application_identity(allocation, "fabric:/Owned", expected, "c" * 64)

    def test_native_ownership_reader_distinguishes_404_absence_from_unavailable_queries(self):
        allocation = MagicMock()
        for status in (200, 204, 404, 409, 500):
            body = (None if status == 204 else {"Error": {"Code": "FABRIC_E_APPLICATION_NOT_FOUND"}}
                    if status == 404 else {"Name": "Owned"})
            allocation.execute.return_value = json.dumps({"status": status, "body": body})
            if status in (200, 204, 404):
                self.assertEqual(data.ownership_read(allocation, "/Applications/Owned"),
                                 {"Name": "Owned"} if status == 200 else None)
            else:
                with self.assertRaisesRegex(RuntimeError, "query failed"):
                    data.ownership_read(allocation, "/Applications/Owned")
        allocation.execute.return_value = json.dumps({"status": 404, "body": {"Name": "not-an-error"}})
        with self.assertRaisesRegex(RuntimeError, "absence is not established"):
            data.ownership_read(allocation, "/Applications/Owned")


class WireDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.partition = str(uuid.uuid4())
        self.engine = str(uuid.uuid4())
        self.peer_engine = str(uuid.uuid4())
        self.own_address, self.peer_address = "127.0.0.1:30001", "127.0.0.1:30002"
        self.message_id = {"guid": str(uuid.uuid4()), "index": 1}
        body = b"offline-frame-fixture:" + uuid.UUID(self.message_id["guid"]).bytes_le
        self.frame = struct.pack("<I", len(body) + 12) + b"\0" * 8 + body

    def endpoint(self, replica, engine, address):
        return f"{address}/{self.partition}-{replica};{engine}"

    def fixture(self, role="Primary", kind="wrong_sender"):
        own = self.endpoint(1, self.engine, self.own_address)
        peer = self.endpoint(2, self.peer_engine, self.peer_address)
        source = {"replica": 1, "role": role, "node": "N0020", "rpc": {
            "replication_address": own, "engine_incarnation": self.engine, "incarnation": "runtime",
            "host": "host", "pid": 123, "engine_kind": "rust",
            "native_epoch": {"data_loss_number": 1, "configuration_number": 2**33 + 7},
            "engine": {"progress": {"assigned": 10}, "copy_done": True, "peers": [
                {"address": peer, "configured": True, "building": False, "cleanup_required": False}]},
            "observed_primary": peer}}
        destination = {"replica": 2, "node": "N0010",
                       "role": "ActiveSecondary" if role == "Primary" else "Primary", "rpc": {
            "incarnation": "native-runtime", "engine_incarnation": self.peer_engine, "host": "native",
            "pid": 124, "engine_kind": "native-classic-v1", "replication_bind_address": "127.0.0.1:0"}}
        before = {"partition": self.partition, "replicas": [source, destination]}
        reply = {"qualification_only": True, "admission_only": False, "kind": kind, "from": own,
                 "target": peer, "native_selected_peer": peer, "sequence": 11,
                 "native_epoch": {"data_loss_number": 1, "configuration_number": 2**33},
                 "sent_epoch": {"data_loss_number": 1, "configuration_number": 2**33},
                 "delivery": {"classification": "socket_write_all_and_flush", "session": 7,
                              "local": "127.0.0.1:40001", "remote": self.peer_address,
                              "remote_listener": self.peer_address, "message_id": self.message_id,
                              "frame_hex": self.frame.hex(), "frame_bytes": len(self.frame)}}
        if kind == "wrong_sender":
            reply["from"] = self.endpoint(1, str(uuid.uuid4()), self.own_address)
        elif kind == "wrong_target":
            reply["target"] = self.endpoint(2, str(uuid.uuid4()), self.peer_address)
        elif kind == "stale_epoch":
            reply["sent_epoch"]["configuration_number"] -= 2**32
        if role == "ActiveSecondary":
            reply["delivery"].update(local=self.own_address, remote="127.0.0.1:40002")
        packets = [
            {"local": reply["delivery"]["local"], "remote": reply["delivery"]["remote"],
             "sequence": (2**32 - 4 + offset) % 2**32, "payload_hex": part.hex()}
            for offset, part in ((0, self.frame[:7]), (7, self.frame[7:23]), (23, self.frame[23:]))]
        observation = {"classification": "destination_kernel_ingress", "interface": "lo",
                       "kernel_drops": 0, "allowed": [[self.own_address, self.peer_address]],
                       "seen_packets": 3, "wire_bytes": len(self.frame) + 120,
                       "packets": packets, "payload_bytes": len(self.frame)}
        return before, source, reply, observation

    def test_exact_segmented_reordered_and_retransmitted_frames_in_both_native_directions(self):
        for role, kinds in (("Primary", ("wrong_sender", "wrong_target", "stale_epoch")),
                            ("ActiveSecondary", ("wrong_sender", "wrong_target", "stale_copy_ack"))):
            for kind in kinds:
                with self.subTest(role=role, kind=kind):
                    before, source, reply, observation = self.fixture(role, kind)
                    observation["packets"] = [observation["packets"][2], observation["packets"][0],
                                              observation["packets"][1], observation["packets"][0]]
                    observation["payload_bytes"] += len(self.frame[:7])
                    result = data.wire_receipt(before, source, kind, reply, observation)
                    self.assertEqual(result["destination_kind"], "native-classic-v1")
                    delivery = result["wire_delivery"]
                    self.assertEqual(delivery["classification"],
                                     "destination_kernel_ingress_not_native_parse_or_rejection")
                    self.assertEqual(delivery["frame_sha256"], hashlib.sha256(self.frame).hexdigest())
                    self.assertEqual(delivery["first_tcp_sequence"], 2**32 - 4)

    def test_admitted_but_dropped_packet_and_absent_observation_are_missing_not_pass(self):
        for missing in ("completion", "destination", "bytes"):
            before, source, reply, observation = self.fixture()
            if missing == "completion":
                reply["admission_only"] = True
                del reply["delivery"]
            elif missing == "destination":
                observation = None
            else:
                observation.update(packets=[], payload_bytes=0)
            result = {"profile": "rust", "errors": [], "application": "fabric:/Owned",
                      "scenarios": {name: {"status": "PASS"} for name in data.required_scenarios("rust")}}
            qualification = data.Qualification(MagicMock(), MagicMock(), result, False)
            qualification.scenario("stale_wire_receipts",
                lambda _: data.wire_receipt(before, source, "wrong_sender", reply, observation))
            self.assertEqual(result["scenarios"]["stale_wire_receipts"]["status"], "MISSING")
            self.assertEqual(data.finalize(result)["status"], "FAIL")

    def test_partial_wrongtuple_wrongbytes_gapped_and_later_healthy_traffic_cannot_prove_delivery(self):
        for invalid in ("partial", "wrongtuple", "wrongbytes", "gap", "healthy"):
            before, source, reply, observation = self.fixture()
            if invalid == "partial":
                observation["packets"].pop()
            elif invalid == "wrongtuple":
                for packet in observation["packets"]:
                    packet["local"] = "127.0.0.1:40002"
            elif invalid == "wrongbytes":
                observation["packets"][-1]["payload_hex"] = "ff" + observation["packets"][-1]["payload_hex"][2:]
            elif invalid == "gap":
                observation["packets"][-1]["sequence"] += 1
            else:
                healthy = self.frame.replace(uuid.UUID(self.message_id["guid"]).bytes_le, uuid.uuid4().bytes_le)
                observation["packets"] = [{**observation["packets"][0], "payload_hex": healthy.hex()}]
            observation["payload_bytes"] = sum(len(bytes.fromhex(p["payload_hex"])) for p in observation["packets"])
            with self.subTest(invalid=invalid), self.assertRaises(data.MissingEvidence):
                data.wire_receipt(before, source, "wrong_sender", reply, observation)

    def test_conflicting_retransmission_loss_wrong_session_or_capture_scope_rejected(self):
        for invalid in ("conflict", "drops", "session", "scope", "message_id", "listener", "accounting"):
            before, source, reply, observation = self.fixture()
            if invalid == "conflict":
                packet = {**observation["packets"][0], "payload_hex": "ff" + observation["packets"][0]["payload_hex"][2:]}
                observation["packets"].append(packet)
                observation["payload_bytes"] += len(bytes.fromhex(packet["payload_hex"]))
            elif invalid == "drops":
                observation["kernel_drops"] = 1
            elif invalid == "session":
                reply["delivery"]["session"] = 0
            elif invalid == "scope":
                observation["allowed"] = [[self.own_address, "127.0.0.1:30003"]]
            elif invalid == "message_id":
                reply["delivery"]["message_id"] = {"guid": str(uuid.uuid4()), "index": 1}
            elif invalid == "listener":
                reply["delivery"]["remote_listener"] = "127.0.0.1:30003"
            else:
                observation["payload_bytes"] += 1
            with self.subTest(invalid=invalid), self.assertRaises(RuntimeError):
                data.wire_receipt(before, source, "wrong_sender", reply, observation)

    def test_retained_native_mapping_guards_cannot_be_bypassed_by_matching_wire_bytes(self):
        for invalid in ("unconfigured", "native_identity", "native_host", "sender", "target", "epoch"):
            before, source, reply, observation = self.fixture()
            if invalid == "unconfigured":
                source["rpc"]["engine"]["peers"][0]["configured"] = False
            elif invalid == "native_identity":
                before["replicas"][1]["replica"] = 3
            elif invalid == "native_host":
                before["replicas"][1]["rpc"]["replication_bind_address"] = "127.0.0.2:0"
            elif invalid == "sender":
                reply["from"] = source["rpc"]["replication_address"]
            elif invalid == "target":
                reply["target"] = self.endpoint(3, self.peer_engine, self.peer_address)
            else:
                reply["native_epoch"]["configuration_number"] = 0
            with self.subTest(invalid=invalid), self.assertRaises(RuntimeError):
                data.wire_receipt(before, source, "wrong_sender", reply, observation)


class WireCaptureTests(unittest.TestCase):
    def local_capture(self, allowed, evidence, *, protocol_only=False):
        allocation = MagicMock()
        allocation.verify.return_value = "d" * 64
        original_popen = subprocess.Popen
        processes = []
        def launch(command, **kwargs):
            self.assertEqual(command[:4], ["docker", "exec", "-i", "d" * 64])
            self.assertEqual(command[4:7], ["python3", "-c", data.WIRE_CAPTURE])
            arguments = [sys.executable, *command[5:]]
            if protocol_only:
                # This fixture tests process ownership/cancellation only. It
                # is never used as destination-wire qualification evidence.
                arguments[2] = """
import json,os,signal,sys,time
signal.alarm(3)
print(json.dumps({"pid":os.getpid(),"token":sys.argv[3],
    "start_time":open("/proc/self/stat").read().rsplit(") ",1)[1].split()[19],
    "classification":"destination_kernel_ingress","interface":"lo",
    "netns_inode":os.stat("/proc/self/ns/net").st_ino,"allowed":json.loads(sys.argv[1])}),flush=True)
os.read(0,16)
"""
            process = original_popen(arguments, **kwargs)
            processes.append(process)
            return process
        def execute(*args, **kwargs):
            self.assertEqual(args[:3], ("python3", "-c", data.STOP_CAPTURE_HELPER))
            stopped = original_popen([sys.executable, *args[1:]], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = stopped.communicate(timeout=kwargs["timeout"])
            self.assertEqual(stopped.returncode, 0, stderr.decode())
            return stdout.decode()
        allocation.execute.side_effect = execute
        return allocation, launch, processes, data.WireCapture(allocation, allowed, evidence)

    def test_stop_helper_cannot_signal_unrelated_process_with_wrong_nonce_or_start_time(self):
        with open("/proc/self/stat") as status:
            start = status.read().rsplit(") ", 1)[1].split()[19]
        for start_time in (start, str(int(start) + 1)):
            proof = {"pid": os.getpid(), "start_time": start_time, "token": "e" * 64}
            with patch("sys.argv", ["stop", json.dumps(proof)]), \
                 patch.object(signal, "pidfd_send_signal") as send:
                with self.assertRaisesRegex(RuntimeError, "incarnation changed"):
                    exec(data.STOP_CAPTURE_HELPER, {})
            send.assert_not_called()

    def test_original_helper_observes_loopback_destination_bytes_without_sender_receipt_input(self):
        with data.socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            peer = "127.0.0.1:" + str(listener.getsockname()[1])
            evidence = {}
            allocation, launch, processes, capture = self.local_capture([["127.0.0.1:30001", peer]], evidence)
            try:
                with patch.object(data.subprocess, "Popen", side_effect=launch), capture:
                    with data.socket.create_connection(listener.getsockname()) as client:
                        server, _ = listener.accept()
                        with server:
                            payload = b"independent-offline-observer-" + uuid.uuid4().bytes
                            client.sendall(payload)
                            self.assertEqual(server.recv(4096), payload)
                            observation = capture.finish()
                            matching = [packet for packet in observation["packets"]
                                        if packet["local"] == "127.0.0.1:" + str(client.getsockname()[1])
                                        and packet["remote"] == peer]
                            self.assertTrue(any(payload in bytes.fromhex(packet["payload_hex"]) for packet in matching))
            except data.MissingEvidence as error:
                if "Operation not permitted" in str(error):
                    self.skipTest("current credentials lack AF_PACKET capability: " + str(error))
                raise
            self.assertTrue(all(process.poll() is not None for process in processes))
            self.assertEqual(evidence["observer_receipt"]["classification"], "destination_kernel_ingress")
            self.assertEqual(evidence["observer_ready"]["token"], evidence["observer_receipt"]["helper"]["token"])
            allocation.execute.assert_called_once()

    def test_cancelled_capture_signals_only_original_pidfd_and_joins_client(self):
        evidence = {}
        allocation, launch, processes, capture = self.local_capture(
            [["127.0.0.1:30001", "127.0.0.1:30002"]], evidence, protocol_only=True)
        started = time.monotonic()
        with patch.object(data.subprocess, "Popen", side_effect=launch):
            with self.assertRaisesRegex(RuntimeError, "cancel fixture"):
                with capture:
                    raise RuntimeError("cancel fixture")
        self.assertLess(time.monotonic() - started, 3)
        self.assertTrue(all(process.poll() is not None for process in processes))
        proof = json.loads(allocation.execute.call_args.args[3])
        self.assertEqual(proof, capture.helper)
        self.assertEqual(evidence["observer_cleanup"], "original helper pidfd exit confirmed")

    def test_allocation_drift_never_authorizes_signalling_an_adopted_helper(self):
        evidence = {}
        allocation, launch, processes, capture = self.local_capture(
            [["127.0.0.1:30001", "127.0.0.1:30002"]], evidence, protocol_only=True)
        with patch.object(data.subprocess, "Popen", side_effect=launch):
            with self.assertRaisesRegex(RuntimeError, "allocation changed"):
                with capture:
                    allocation.verify.return_value = "e" * 64
        allocation.execute.assert_not_called()
        self.assertTrue(all(process.poll() is not None for process in processes))
        self.assertIn("refuse signal", evidence["observer_cleanup_error"])

    def test_readiness_deadline_joins_client_without_pid_or_name_adoption(self):
        import select
        evidence = {}
        allocation, launch, processes, capture = self.local_capture(
            [["127.0.0.1:30001", "127.0.0.1:30002"]], evidence, protocol_only=True)
        started = time.monotonic()
        with patch.object(data.subprocess, "Popen", side_effect=launch), \
                patch.object(select, "select", return_value=([], [], [])):
            with self.assertRaisesRegex(data.MissingEvidence, "readiness deadline"):
                with capture:
                    self.fail("unready capture yielded to probe sender")
        self.assertLess(time.monotonic() - started, 3)
        allocation.execute.assert_not_called()
        self.assertTrue(all(process.poll() is not None for process in processes))
        self.assertIn("no PID/name adoption", evidence["observer_cleanup"])

    def helper_packets(self, packets, *, drops=0):
        import select
        fake = MagicMock()
        fake.recvfrom.side_effect = [*packets, BlockingIOError()]
        fake.getsockopt.return_value = struct.pack("III", 10, drops, 0)
        stdout = io.StringIO()
        allowed = [["127.0.0.1:30001", "127.0.0.1:30002"]]
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(data.socket, "socket", return_value=fake))
            stack.enter_context(patch.object(select, "select", side_effect=[([fake], [], []), ([sys.stdin], [], [])]))
            stack.enter_context(patch.object(signal, "signal"))
            alarm = stack.enter_context(patch.object(signal, "alarm"))
            stack.enter_context(patch.object(os, "read", return_value=b"finish\n"))
            stack.enter_context(patch.object(sys, "argv", ["observer", json.dumps(allowed),
                                                         str(time.time() + 10), "a" * 64]))
            stack.enter_context(contextlib.redirect_stdout(stdout))
            try:
                exec(data.WIRE_CAPTURE, {})
                code = 0
            except SystemExit as error:
                code = error.code
        self.assertEqual(fake.bind.call_args.args, (("lo", 0),))
        fake.close.assert_called_once()
        self.assertGreaterEqual(alarm.call_args.args[0], 1)
        self.assertLessEqual(alarm.call_args.args[0], 20)
        return code, [json.loads(line) for line in stdout.getvalue().splitlines()]

    def datagram(self, payload=b"invalid-fixture", *, packet_type=0, source=40001, destination=30002):
        tcp = struct.pack("!HHIIBBHHH", source, destination, 42, 0, 0x50, 0x18, 4096, 0, 0)
        ip = struct.pack("!BBHHHBBH4s4s", 0x45, 0, 20 + len(tcp) + len(payload), 0, 0, 64, 6, 0,
                         data.socket.inet_aton("127.0.0.1"), data.socket.inet_aton("127.0.0.1"))
        return ip + tcp + payload, ("lo", 0x0800, packet_type, 772, b"")

    def test_helper_filters_destination_ingress_and_keeps_exact_tcp_tuple_sequence_bytes(self):
        code, lines = self.helper_packets([self.datagram(packet_type=4), self.datagram(destination=30003),
                                           self.datagram(), self.datagram(source=30001, destination=40002)])
        self.assertEqual(code, 0)
        self.assertEqual(lines[0]["classification"], "destination_kernel_ingress")
        self.assertEqual(lines[1]["packets"], [
            {"local": "127.0.0.1:40001", "remote": "127.0.0.1:30002",
             "sequence": 42, "payload_hex": b"invalid-fixture".hex()},
            {"local": "127.0.0.1:30001", "remote": "127.0.0.1:40002",
             "sequence": 42, "payload_hex": b"invalid-fixture".hex()}])
        self.assertEqual(lines[1]["payload_bytes"], 2 * len(b"invalid-fixture"))

    def test_helper_packet_bound_loss_and_truncation_are_failures_not_evidence(self):
        for invalid in ("packets", "drops", "truncated"):
            packets = ([self.datagram()] * 1025 if invalid == "packets" else [self.datagram()])
            if invalid == "truncated":
                packet, address = packets[0]
                packets[0] = (packet[:-1], address)
            code, lines = self.helper_packets(packets, drops=1 if invalid == "drops" else 0)
            self.assertEqual(code, 1)
            self.assertIn("error", lines[-1])
            self.assertNotIn("packets", lines[-1])


class ReportingTests(unittest.TestCase):
    def test_summary_console_retains_complete_report_without_allocating_or_weakening_status(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            result = {"profile": "rust", "errors": [], "cleanup": {}, "large_evidence": "X" * 100000,
                      "scenarios": {name: {"status": "PASS"} for name in data.required_scenarios("rust")}}
            stdout = io.StringIO()
            with patch.object(sys, "argv", ["fube_data_test", "summary-model", "--prebuilt", directory,
                    "--existing-owner", "a" * 32, "--profile", "rust", "--report", str(report), "--summary"]), \
                 patch.object(data, "load_bundle"), patch.object(data.runtime, "Allocation"), \
                 patch.object(data, "qualify", return_value=result), \
                 patch.object(data.runtime, "create_owned") as create, \
                 patch.object(data.runtime, "destroy_owned") as destroy, contextlib.redirect_stdout(stdout):
                data.main()
            self.assertEqual(json.loads(report.read_text())["large_evidence"], "X" * 100000)
            self.assertEqual(json.loads(stdout.getvalue())["status"], "PASS")
            self.assertNotIn("large_evidence", stdout.getvalue())
            self.assertLess(len(stdout.getvalue()), 4000)
            create.assert_not_called()
            destroy.assert_not_called()

    def test_native_data_is_truthfully_partial_without_changing_full_or_mixed_inventories(self):
        names = data.required_scenarios("native-data")
        self.assertEqual(len(names), 17)
        self.assertEqual(names, data.required_scenarios("mixed"))
        self.assertNotIn("legacy_full", names)
        self.assertEqual(data.required_scenarios("full"), names + ("legacy_full",))
        value = {"profile": "native-data", "errors": [], "scenarios": {
            name: {"status": "PASS"} for name in names}}
        self.assertEqual(data.finalize(value)["status"], "PASS")
        value["profile"] = "full"
        self.assertEqual(data.finalize(value)["status"], "FAIL")
        self.assertEqual(value["missing_or_failed"], ["legacy_full"])

    def test_native_ci_executes_two_whole_profiles_and_hardened_legacy_with_strict_aggregate(self):
        workflow = (data.ROOT / ".github/workflows/ci.yaml").read_text()
        def step(identifier):
            return workflow.split("id: " + identifier + "\n", 1)[1].split("\n    - ", 1)[0]
        self.assertIn("fail-fast: false", workflow)
        block = step("partition")
        self.assertIn("-m tests.run", block)
        self.assertIn('--partition "$WORKER_ROUTE"', block)
        self.assertIn('--prebuilt "$RUNNER_TEMP/fube-data-binaries"', block)
        self.assertIn('--github-output "$GITHUB_OUTPUT"', block)
        self.assertIn('--bundle-tar "$RUNNER_TEMP/fube-data-artifact/fube-data-binaries.tar"', block)
        self.assertNotIn("--owner", block)
        self.assertNotIn("id: create_cluster", workflow)
        self.assertNotIn("id: health", workflow)
        self.assertIn("if: always() && steps.partition.outputs.owner != ''", workflow)
        self.assertIn("steps.partition.outputs.cleaned != 'true'", workflow)
        self.assertNotIn("cargo ", block)
        self.assertNotIn("rustc ", block)
        self.assertIn("fromJSON(needs.rust-build.outputs.onebox-matrix)", workflow)
        self.assertIn("artifact-ids: ${{ needs.onebox-workers.outputs.client-runtime-artifact }},"
                      "${{ needs.onebox-workers.outputs.native-data-artifact }}", workflow)
        self.assertNotIn("pattern: onebox-worker-", workflow)
        self.assertIn('--worker-attempts-json "$WORKER_ATTEMPTS"', workflow)
        self.assertIn('echo "$WORKER_ROUTE=$ARTIFACT_ID" >> "$GITHUB_OUTPUT"', workflow)
        self.assertIn('echo "$WORKER_ROUTE-attempt=$GITHUB_RUN_ATTEMPT" >> "$GITHUB_OUTPUT"', workflow)
        self.assertIn("merge-multiple: true", workflow)
        self.assertNotIn("name: onebox-client-runtime-", workflow)
        self.assertNotIn("name: onebox-native-data-", workflow)
        self.assertIn("name: Linux onebox native client", workflow)
        self.assertIn("needs: [rust-build, onebox-workers, rust-tests, rust-quality, python-tests, standalone]", workflow)
        self.assertIn("-m tests.onebox.shared.ci aggregate", workflow)
        self.assertIn("-m tests.onebox.shared.ci cleanup", workflow)
        self.assertLess(workflow.index("name: Emergency cleanup"), workflow.index("name: Upload this worker"))
        self.assertNotIn("continue-on-error", workflow)

    def test_ci_input_downloads_use_the_published_producer_attempt_on_reruns(self):
        workflow = (data.ROOT / ".github/workflows/ci.yaml").read_text()
        producer = (data.ROOT / ".github/workflows/rust-build.yaml").read_text()
        standalone = (data.ROOT / ".github/workflows/native-runtime.yaml").read_text()
        self.assertIn("producer-attempt: ${{ steps.producer.outputs.attempt }}", producer)
        self.assertIn('echo "attempt=$GITHUB_RUN_ATTEMPT" >> "$GITHUB_OUTPUT"', producer)
        for name in ("native-runtime-binaries", "fube-data-binaries", "rust-runtime-binaries", "rust-tests"):
            self.assertIn(f"name: {name}\n        overwrite: true", producer)
            self.assertNotIn(f"name: {name}-${{{{ github.run_id }}}}", producer)
        self.assertIn("FUBE_CI_PRODUCER_ATTEMPT: ${{ needs.rust-build.outputs.producer-attempt }}", workflow)
        self.assertIn("name: onebox-inputs-${{ github.run_id }}-"
                      "${{ needs.rust-build.outputs.producer-attempt }}", workflow)
        self.assertNotIn("name: onebox-inputs-${{ github.run_id }}-${{ github.run_attempt }}", workflow)
        self.assertIn("producer-attempt: ${{ needs.rust-build.outputs.producer-attempt }}", workflow)
        self.assertIn("FUBE_CI_PRODUCER_ATTEMPT: ${{ inputs.producer-attempt }}", standalone)
        self.assertIn("name: native-runtime-binaries\n", standalone)
        self.assertIn("name: onebox-inputs-${{ github.run_id }}-${{ inputs.producer-attempt }}", standalone)

    def result(self, profile):
        return {"profile": profile, "status": "FAIL", "errors": [],
                "scenarios": {name: {"status": "PASS"} for name in data.required_scenarios(profile)}}

    def test_pass_requires_every_required_scenario_not_skips_errors_or_omissions(self):
        for profile in ("rust", "mixed", "full", "native-data"):
            self.assertEqual(data.finalize(self.result(profile))["status"], "PASS")
            for name in data.required_scenarios(profile):
                for status in ("MISSING", "FAIL", "SKIPPED", "PASS_SOURCE_ONLY"):
                    value = self.result(profile)
                    value["scenarios"][name]["status"] = status
                    self.assertEqual(data.finalize(value)["status"], "FAIL")
                value = self.result(profile)
                del value["scenarios"][name]
                self.assertEqual(data.finalize(value)["status"], "FAIL")
            value = self.result(profile)
            value["errors"].append("cleanup failed")
            self.assertEqual(data.finalize(value)["status"], "FAIL")
        full = self.result("full")
        full["scenarios"]["legacy_full"]["status"] = "MISSING"
        self.assertEqual(data.finalize(full)["status"], "FAIL")

    def test_missing_live_probe_and_failed_scenario_never_get_relabelled_pass(self):
        result = self.result("rust")
        qualification = data.Qualification(MagicMock(), MagicMock(), {**result, "application": "fabric:/Owned"}, False)
        record = qualification.ledger.mutation("retained-after-failure")
        record["attempts"].append({"outcome": "observation_unavailable", "error": "actual cause"})
        def missing(evidence):
            evidence["actual_writes"] = 100
            raise data.MissingEvidence("no native stale-actor injection")
        qualification.scenario("stale_wire_receipts", missing)
        self.assertEqual(result["scenarios"]["stale_wire_receipts"]["status"], "MISSING")
        self.assertEqual(data.finalize(result)["status"], "FAIL")
        self.assertIs(qualification.result["identified_workload"]["retained-after-failure"], record)

    def test_interrupted_scenario_keeps_captured_evidence_and_never_stays_running(self):
        result = self.result("rust")
        qualification = data.Qualification(
            MagicMock(), MagicMock(), {**result, "application": "fabric:/Owned"}, False)
        def interrupted(evidence):
            evidence["original_native_instance"] = "captured-before-interruption"
            raise KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            qualification.scenario("deployment_identity", interrupted)
        evidence = result["scenarios"]["deployment_identity"]
        self.assertEqual(evidence["status"], "FAIL")
        self.assertEqual(evidence["original_native_instance"], "captured-before-interruption")
        self.assertIn("KeyboardInterrupt", evidence["error"])
        self.assertIn("finished_at", evidence)
        self.assertEqual(data.finalize(result)["status"], "FAIL")

    def test_failed_deployment_skips_unusable_scenarios_without_claiming_pass(self):
        result = self.result("mixed")
        result["application"] = "fabric:/Owned"
        result["scenarios"] = {name: {"status": "MISSING", "reason": "not executed"}
                               for name in data.required_scenarios("mixed")}
        qualification = data.Qualification(MagicMock(), MagicMock(), result, True)
        def failed(evidence):
            evidence["activation_error"] = "selected prebuilt engine is not executable"
            raise RuntimeError(evidence["activation_error"])
        with patch.object(qualification, "deployment_identity", side_effect=failed), \
             patch.object(qualification, "writes_100") as writes, \
             patch.object(qualification, "direction") as direction:
            qualification.run()
        writes.assert_not_called()
        direction.assert_not_called()
        self.assertEqual(result["scenarios"]["deployment_identity"]["status"], "FAIL")
        self.assertTrue(all(value["status"] == "MISSING" for name, value in
                            result["scenarios"].items() if name != "deployment_identity"))
        self.assertEqual(data.finalize(result)["status"], "FAIL")

    def test_provenance_never_relabels_review_or_ci_heads_as_executables(self):
        bundle = MagicMock(source_revision="a" * 40, manifest_sha256="b" * 64)
        with patch.dict(os.environ, {"FUBE_REVIEW_HEAD": "c" * 40, "FUBE_CI_HEAD": "d" * 40,
                                    "FUBE_CI_MERGE": "e" * 40}, clear=True):
            value = data.provenance(bundle)
        self.assertEqual(value["executable_source_revision"], "a" * 40)
        self.assertEqual(value["review_head"], "c" * 40)
        self.assertEqual(value["ci_head"], "d" * 40)
        self.assertEqual(value["ci_merge"], "e" * 40)

    def test_failed_baseline_writes_leave_dependent_qualification_missing(self):
        result = {"application": "fabric:/Owned", "profile": "mixed", "errors": [],
                  "scenarios": {name: {"status": "MISSING"} for name in data.required_scenarios("mixed")}}
        qualification = data.Qualification(MagicMock(), MagicMock(), result, True)
        def failed(evidence):
            qualification.ledger.mutation("baseline-failed")["attempts"].append({"error": "cause"})
            raise RuntimeError("actual write rejection")
        with patch.object(qualification, "deployment_identity"), \
             patch.object(qualification, "writes_100", side_effect=failed), \
             patch.object(qualification, "identified_retry") as retry:
            qualification.run()
        retry.assert_not_called()
        self.assertEqual(result["scenarios"]["writes_100"]["status"], "FAIL")
        self.assertEqual(result["scenarios"]["planned_move"]["status"], "MISSING")
        self.assertIn("baseline-failed", result["identified_workload"])
        self.assertEqual(data.finalize(result)["status"], "FAIL")

class ExecutedImageStoreContractTests(unittest.TestCase):
    def test_native_omits_only_exact_schema_default_service_attributes(self):
        prefix=f'<ApplicationManifest xmlns="{data.runtime.NS}" ApplicationTypeName="TestType" ApplicationTypeVersion="1.1"><Parameters><Parameter Name="FubeDataTypeCapability" DefaultValue="{"a"*64}"/></Parameters>'
        explicit=prefix+'<DefaultServices><Service Name="Bootstrap" ServicePackageActivationMode="SharedProcess"><StatefulService ServiceTypeName="Type" TargetReplicaSetSize="1" MinReplicaSetSize="1"><SingletonPartition/></StatefulService></Service></DefaultServices></ApplicationManifest>'
        omitted=explicit.replace(' ServicePackageActivationMode="SharedProcess"','').replace(' TargetReplicaSetSize="1"','').replace(' MinReplicaSetSize="1"','')
        self.assertEqual(data.manifest_identity(explicit),data.manifest_identity(omitted))
        for changed in [explicit.replace('MinReplicaSetSize="1"','MinReplicaSetSize="2"'),
                        explicit.replace('SharedProcess','ExclusiveProcess'),
                        explicit.replace('ServiceTypeName="Type"','ServiceTypeName="Other"')]:
            self.assertNotEqual(data.manifest_identity(explicit),data.manifest_identity(changed))

    def test_native_upload_absence_is_independent_read_only_and_exact_container_bound(self):
        allocation=MagicMock()
        allocation.verify.return_value="cid"
        upload=data.UploadOwnership("/var/lib/sf-node/fube-data-"+"1"*32,"cid",(1,2),(1,3),(1,4),"a"*64)
        allocation.execute.return_value=json.dumps({"status":404,"body":None})
        proof=data.verify_image_store_upload_absent(allocation,upload)
        self.assertTrue(proof["absence_verified"])
        self.assertEqual(proof["path"],"fube-data-"+"1"*32)
        allocation.sf.assert_not_called()
        for reply in [{"status":200,"body":{"StoreFiles":[],"StoreFolders":[]}},
                      {"status":204,"body":None},{"status":503,"body":None}]:
            allocation.execute.return_value=json.dumps(reply)
            with self.assertRaises(RuntimeError):
                data.verify_image_store_upload_absent(allocation,upload)
        allocation.verify.return_value="replacement"
        with self.assertRaises(RuntimeError):
            data.verify_image_store_upload_absent(allocation,upload)

    def test_non_pull_request_ci_has_no_fabricated_merge_commit(self):
        bundle=MagicMock(source_revision="a"*40,manifest_sha256="b"*64)
        with patch.dict(os.environ,{"FUBE_CI_HEAD":"c"*40,"FUBE_CI_MERGE":"",
                                   "FUBE_CI_RUN_ID":"42"},clear=True):
            proof=data.provenance(bundle)
        self.assertIsNone(proof["ci_merge"])
        self.assertEqual(proof["ci_head"],"c"*40)
        self.assertEqual(proof["ci_run_id"],"42")
        self.assertRegex(proof["qualification_script_sha256"],"^[a-f0-9]{64}$")

    def test_native_parameter_map_order_preserves_exact_values_and_generation_identity(self):
        text=f'<ApplicationManifest xmlns="{data.runtime.NS}" ApplicationTypeName="TestType" ApplicationTypeVersion="1.1"><Parameters><Parameter Name="FubeDataTypeCapability" DefaultValue="{"a"*64}"/><Parameter Name="FubeDataCreationCapability" DefaultValue=""/></Parameters><DefaultServices/></ApplicationManifest>'
        root=ET.fromstring(text)
        parameters=root.find(f"{{{data.runtime.NS}}}Parameters")
        parameters[:]=list(reversed(list(parameters)))
        self.assertEqual(data.manifest_identity(text),data.manifest_identity(ET.tostring(root,encoding="unicode")))
        parameters[0].set("DefaultValue","different")
        self.assertNotEqual(data.manifest_identity(text),data.manifest_identity(ET.tostring(root,encoding="unicode")))

    def test_native_empty_404_is_scoped_to_the_pinned_manifest_query(self):
        allocation=MagicMock()
        allocation.execute.return_value=json.dumps({"status":404,"body":None})
        path="/ImageStore/Store%2FFubeDataExampleType%2FApplicationManifest.1.1.xml?api-version=6.2"
        self.assertIsNone(data.ownership_read(allocation,path))
        for invalid in ["/Applications/FubeDataExample?api-version=6.0",
                        "/ImageStore/Store?api-version=6.2",
                        "/ImageStore/Store%2FFubeDataExampleType%2FApplicationManifest.1.1.xml?api-version=6.0"]:
            with self.assertRaises(RuntimeError):
                data.ownership_read(allocation,invalid)

    def test_empty_404_never_becomes_positive_original_generation_proof(self):
        allocation=MagicMock()
        allocation.execute.return_value=json.dumps({"status":404,"body":None})
        expected=data.TypeIdentity("FubeDataExampleType","1.1","a"*64,"b"*64)
        self.assertIsNone(data.native_type_generation(allocation,expected,allow_absent=True))
        with self.assertRaisesRegex(RuntimeError,"positive native"):
            data.native_type_generation(allocation,expected)


if __name__ == "__main__":
    unittest.main()
