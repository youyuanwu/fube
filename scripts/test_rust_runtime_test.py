"""Offline ownership/qualification regressions; no native interoperability claim."""
import contextlib
import copy
import io
import json
import shlex
import sys
import textwrap
import unittest
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock, ANY

import rust_runtime_test as runtime

def model_binaries():
    return runtime.Binaries(Path("/prebuilt")/"app",Path("/prebuilt")/"observer","native-free",
                            Path("/prebuilt")/"templates","a"*64,"1"*40)


@unittest.skipUnless(sys.platform=="linux","Linux-only application-runtime harness")
class BinaryBundleTests(unittest.TestCase):
    def setUp(self):
        source=patch.object(runtime,"clean_source_revision",return_value="1"*40)
        source.start()
        self.addCleanup(source.stop)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.app = self.root / "app"
        self.observer = self.root / "observer"
        for binary in (self.app, self.observer):
            binary.write_bytes(binary.name.encode())
            binary.chmod(0o755)
        self.binaries = runtime.Binaries(self.app, self.observer, "rust-runtime-app\n  mssf-runtime")

    def bundle(self, name="bundle"):
        directory = self.root / name
        with patch.object(runtime, "build_binaries", return_value=self.binaries):
            runtime.build_bundle(directory)
        return directory

    def test_strict_version_profile_revision_and_keys_reject_old_or_ambiguous_bundles(self):
        for field,value in [("version",2),("profile","stateless"),("source_revision","dirty"),("extra",True)]:
            directory=self.bundle("metadata-"+field)
            path=directory/"build-info.json"
            metadata=json.loads(path.read_text());metadata[field]=value
            path.write_text(json.dumps(metadata))
            with self.assertRaises(RuntimeError):runtime.load_bundle(directory)

    def test_dirty_source_is_rejected_before_build_or_allocation(self):
        with patch.object(runtime,"clean_source_revision",side_effect=RuntimeError("dirty source")), \
             patch.object(runtime,"build_binaries") as build:
            with self.assertRaises(RuntimeError):runtime.build_bundle(self.root/"dirty")
            build.assert_not_called()

    def test_build_only_produces_verified_executables_without_allocation(self):
        directory = self.root / "bundle"
        with patch("sys.argv", ["rust_runtime_test.py", "--build-bundle", str(directory)]), \
             patch.object(runtime, "build_binaries", return_value=self.binaries), \
             patch.object(runtime, "create_owned") as create, \
             contextlib.redirect_stdout(io.StringIO()):
            runtime.main()
            create.assert_not_called()
        with patch.object(runtime, "run", side_effect=AssertionError("bundle loading must not invoke Cargo")):
            binaries = runtime.load_bundle(directory)
        self.assertEqual(binaries.app.read_bytes(), self.app.read_bytes())
        self.assertEqual(binaries.observer.read_bytes(), self.observer.read_bytes())
        self.assertEqual(binaries.dependency_graph, self.binaries.dependency_graph)
        self.assertEqual(binaries.app.stat().st_mode & 0o777, 0o755)
        self.assertEqual(binaries.observer.stat().st_mode & 0o777, 0o755)

    def test_invalid_prebuilt_fails_before_allocation_without_build_fallback(self):
        for failure in ("missing-bundle", "missing-observer", "tampered-app", "missing-graph", "native-graph"):
            with self.subTest(failure=failure):
                directory = self.bundle(failure)
                if failure == "missing-bundle":
                    directory = directory / "absent"
                elif failure == "missing-observer":
                    (directory / "runtime_observer").unlink()
                elif failure == "tampered-app":
                    (directory / "rust-runtime-app").write_bytes(b"replaced after build")
                else:
                    path = directory / "build-info.json"
                    metadata = json.loads(path.read_text())
                    metadata["dependency_graph"] = "" if failure == "missing-graph" else "mssf-core"
                    path.write_text(json.dumps(metadata))
                with patch("sys.argv", ["rust_runtime_test.py", "owned", "--prebuilt", str(directory)]), \
                     patch.object(runtime, "build_binaries") as build, \
                     patch.object(runtime, "create_owned") as create, \
                     patch.object(runtime, "Allocation") as allocation, \
                     contextlib.redirect_stdout(io.StringIO()) as output:
                    with self.assertRaises(SystemExit):
                        runtime.main()
                    build.assert_not_called()
                    create.assert_not_called()
                    allocation.assert_not_called()
                    self.assertEqual(json.loads(output.getvalue())["status"], "FAIL")

    def test_prebuilt_qualification_uses_bundle_app_and_observer_without_cargo(self):
        binaries = runtime.load_bundle(self.bundle())
        allocation = MagicMock()
        allocation.name = "owned"
        allocation.owner = "capability"
        allocation.container_id = "cid"
        allocation.sf.return_value = '{"items":[]}'
        def run(*args, **_kwargs):
            self.assertNotEqual(args[0], "cargo")
            return ""
        with patch.object(runtime, "run", side_effect=run), \
             patch.object(runtime, "build_binaries", side_effect=AssertionError("must not build")), \
             patch.object(runtime, "package", return_value=(self.root, "minted-type", "1.1")) as package, \
             patch.object(runtime, "restart", return_value={"native": "model observation"}) as restart, \
             patch.object(runtime, "configuration_upgrade", side_effect=lambda a,b,result,attempts:
                          result.update(configuration_upgrade={"final":{"rpc":{"packages":{"generation":1}}}})) as upgrade, \
             patch.object(runtime, "controls_and_authority") as controls:
            result = runtime.qualify(allocation, True, binaries)
        self.assertEqual(result["status"], "PASS_FULL_STATELESS_PROFILE")
        self.assertEqual(package.call_args.args[1], binaries.app)
        self.assertEqual(package.call_args.kwargs["templates"], binaries.template_root)
        self.assertEqual(upgrade.call_args.args[1], binaries)
        self.assertEqual(restart.call_args.kwargs["observer"], binaries.observer)
        self.assertEqual(controls.call_args.kwargs["observer"], binaries.observer)
        self.assertEqual(result["dependency_graph"], binaries.dependency_graph)
        self.assertEqual(result["binary_sha256"]["rust-runtime-app"], runtime.digest(binaries.app))
        observed = {"resolver": "mssf-client", "rpc": {"service": "fabric:/Owned/Singleton", "pid": 7},
                    "native_pid": 7}
        with patch.object(runtime, "run", return_value=json.dumps(observed)) as command:
            runtime.observe("fabric:/Owned/Singleton", observer=binaries.observer)
        self.assertEqual(command.call_args.args[0], str(binaries.observer))

    def test_templates_are_strict_prebuilt_inputs(self):
        for failure in ("missing","extra","tampered","symlink","fifo","metadata-fifo"):
            directory = self.bundle(f"templates-{failure}")
            path = directory / "package-v2/RustRuntimeService/Config/Settings.xml"
            if failure == "missing":
                path.unlink()
            elif failure == "extra":
                (directory / "unexpected.xml").write_text("<not-allowed/>")
            elif failure == "tampered":
                path.write_text("<tampered/>")
            elif failure == "symlink":
                path.unlink()
                path.symlink_to(directory / "package-v1/RustRuntimeService/Config/Settings.xml")
            elif failure == "fifo":
                runtime.os.mkfifo(directory / "package-v2/unlisted-pipe")
            else:
                manifest=directory / "build-info.json"
                manifest.unlink()
                runtime.os.mkfifo(manifest)
            with patch.object(runtime,"run",side_effect=AssertionError("compiler fallback")):
                with self.assertRaises(RuntimeError):
                    runtime.load_bundle(directory)

    def test_bundled_versions_keep_code_identity_and_update_only_configuration(self):
        binaries=runtime.load_bundle(self.bundle("versioned"))
        with patch.object(runtime,"ROOT",self.root):
            first,first_type,first_version=runtime.package("fixture",binaries.app,templates=binaries.template_root)
            second,second_type,second_version=runtime.package("fixture",binaries.app,templates=binaries.template_root,upgrade=True)
        self.assertEqual((first_type,first_version,second_type,second_version),
                         ("FubeRustRuntimefixtureType","1.1","FubeRustRuntimefixtureType","1.2"))
        self.assertEqual((first/"RustRuntimeService/Code/rust-runtime-app").read_bytes(),
                         (second/"RustRuntimeService/Code/rust-runtime-app").read_bytes())
        self.assertIn("before-upgrade",(first/"RustRuntimeService/Config/Settings.xml").read_text())
        self.assertIn("after-upgrade",(second/"RustRuntimeService/Config/Settings.xml").read_text())

    def test_default_build_is_locked_and_checks_native_dependencies(self):
        with patch.object(runtime, "run", return_value=self.binaries.dependency_graph) as command:
            binaries = runtime.build_binaries()
        self.assertEqual(binaries.dependency_graph, self.binaries.dependency_graph)
        commands = [call.args for call in command.call_args_list]
        self.assertTrue(any(args[:2] == ("cargo", "build") and "rust-runtime-app" in args for args in commands))
        self.assertTrue(any("--example" in args and "runtime_observer" in args for args in commands))
        self.assertTrue(all("--locked" in args for args in commands))
        with patch.object(runtime, "run", return_value="rust-runtime-app\n  mssf-core"):
            with self.assertRaisesRegex(RuntimeError, "native SF adapters"):
                runtime.build_binaries()


class ReportingEvidenceTests(unittest.TestCase):
    def test_load_requires_fm_distinctive_value_and_timestamp(self):
        query = {"PrimaryLoadMetricReports": [
            {"Name": "RuntimeWork", "Value": 731, "LastReportedUtc": "2026-10-27T01:00:00Z"},
            {"Name": "_MoveCost_", "Value": 3, "LastReportedUtc": "2026-10-27T01:00:00Z"}]}
        self.assertTrue(runtime.load_value(query, "RuntimeWork", 731))
        self.assertTrue(runtime.load_value(query, "_MoveCost_", 3))
        self.assertFalse(runtime.load_value(query, "RuntimeWork", 947))
        query["PrimaryLoadMetricReports"][0]["LastReportedUtc"] = ""
        self.assertFalse(runtime.load_value(query, "RuntimeWork", 731))

    def test_health_source_property_and_duplicate_identity(self):
        query = {"HealthEvents": [
            {"SourceId": "Other", "Property": "Ready"},
            {"SourceId": "RustRuntime", "Property": "Ready", "IsExpired": True}]}
        self.assertTrue(runtime.health_event(query, "Ready")["IsExpired"])
        self.assertIsNone(runtime.health_event(query, "Removed"))
        query["HealthEvents"].append(query["HealthEvents"][1])
        with self.assertRaises(RuntimeError):
            runtime.health_event(query, "Ready")

    def test_partition_fault_replacement_is_not_process_replacement(self):
        before = {"rpc": {"partition": "p", "instance": 5, "incarnation": 6, "pid": 7, "host": "h"}}
        runtime.require_same_instance(copy.deepcopy(before), before)
        for field in before["rpc"]:
            changed = copy.deepcopy(before)
            changed["rpc"][field] = "changed"
            with self.assertRaises(RuntimeError):
                runtime.require_same_instance(changed, before)

    def test_report_command_uses_prebuilt_independent_observer(self):
        observed = {"resolver": "mssf-client", "native_pid": 7,
                    "rpc": {"pid": 7, "service": "fabric:/Owned/Concurrent", "report": {"ok": True}}}
        observer = Path("/bundle") / "runtime_observer"
        with patch.object(runtime, "run", return_value=json.dumps(observed)) as command:
            runtime.observe("fabric:/Owned/Concurrent", "int:0", observer=observer,
                            command="op=fault")
        command.assert_called_once_with(str(observer), "127.0.0.1:19000",
                                       "fabric:/Owned/Concurrent", "int:0", "op=fault", timeout=45)


@unittest.skipUnless(sys.platform=="linux","Linux-only application-runtime harness")
class AllocationTests(unittest.TestCase):
    def setUp(self):
        bundle=patch.object(runtime,"load_bundle",return_value=model_binaries())
        bundle.start(); self.addCleanup(bundle.stop)
        self.state = {"name": "owned", "owner": "capability", "project": "sf-onebox-owned",
                      "runtime_version": "11.8.121.4"}
        self.container = {
            "Id": "immutable-container", "State": {"Running": True},
            "Config": {"Labels": {runtime.OWNER_LABEL: "capability",
                                  "com.docker.compose.project": "sf-onebox-owned",
                                  "com.docker.compose.service": "onebox"}},
            "Mounts": [{"Name": "sf-onebox-owned_data", "Destination": "/var/lib/sf-node"}],
        }

    def test_exact_owner_volume_version_and_running_identity(self):
        self.assertEqual(runtime.validate_allocation(self.state, self.container, "owned", "capability"),
                         "immutable-container")
        for value in ("other", "", "capability-wrong"):
            with self.assertRaises(RuntimeError):
                runtime.validate_allocation(self.state, self.container, "owned", value)
        for field in ("owner", "project", "runtime_version", "name"):
            changed = dict(self.state)
            changed[field] = "wrong"
            with self.assertRaises(RuntimeError):
                runtime.validate_allocation(changed, self.container, "owned", "capability")
        changed = copy.deepcopy(self.container)
        changed["Mounts"][0]["Name"] = "unrelated"
        with self.assertRaises(RuntimeError):
            runtime.validate_allocation(self.state, changed, "owned", "capability")
        changed = copy.deepcopy(self.container)
        changed["State"]["Running"] = False
        with self.assertRaises(RuntimeError):
            runtime.validate_allocation(self.state, changed, "owned", "capability")

    def test_rejected_creation_never_gets_name_only_cleanup(self):
        with patch("sys.argv", ["rust_runtime_test.py", "owned", "--prebuilt", "bundle"]), \
             patch.object(runtime, "create_owned", side_effect=RuntimeError("controller rejected preexisting allocation")) as create, \
             patch.object(runtime,"destroy_owned") as destroy, \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit):
                runtime.main()
            create.assert_called_once_with("owned",on_allocated=ANY)
            destroy.assert_not_called()
        # Rejection before a new allocation commits grants no cleanup authority.

    def test_successful_create_retains_cleanup_after_constructor_failure(self):
        with patch("sys.argv",["rust_runtime_test.py","owned","--prebuilt","bundle"]), \
             patch.object(runtime,"create_owned",return_value="acquired-owner"), \
             patch.object(runtime,"Allocation",side_effect=RuntimeError("post-create status failed")), \
             patch.object(runtime,"destroy_owned",return_value={"phase":"destroyed"}) as destroy, \
             contextlib.redirect_stdout(io.StringIO()) as output:
            with self.assertRaises(SystemExit):
                runtime.main()
            destroy.assert_called_once_with("owned","acquired-owner")
            result=json.loads(output.getvalue())
            self.assertEqual(result["status"],"FAIL")
            self.assertIn("post-create",result["errors"][0])
            self.assertEqual(result["cleanup"]["onebox"]["phase"],"destroyed")

    def test_cleanup_uses_controller_lock_and_owner_not_readiness(self):
        cluster=MagicMock()
        cluster.state={"owner":"acquired-owner","phase":"down"}
        cluster.resources.return_value=([],[],[])
        cluster.destroy.return_value={"phase":"destroyed"}
        with patch.object(runtime.onebox_ctl.ctl,"locked",return_value=contextlib.nullcontext(Path("/fake"))), \
             patch.object(runtime.onebox_ctl,"Onebox",return_value=cluster):
            runtime.destroy_owned("owned","acquired-owner")
            cluster.destroy.assert_called_once_with("owned")
            cluster.destroy.reset_mock()
            cluster.state["owner"]="different-owner"
            with self.assertRaises(RuntimeError):
                runtime.destroy_owned("owned","acquired-owner")
            cluster.destroy.assert_not_called()

    def test_creation_captures_original_owner_before_controller_unlock(self):
        cluster=MagicMock()
        held={"value":False}
        @contextlib.contextmanager
        def lock(_root,_name):
            held["value"]=True
            yield Path("/fake")
            held["value"]=False
            # Earliest permitted replacement is after creation's transaction.
            cluster.state={"owner":"b"*32}
        def create(*_args,on_allocated):
            self.assertTrue(held["value"])
            cluster.state={"owner":"a"*32}
            on_allocated(cluster.state["owner"])
            cluster.state={"owner":"c"*32}
        cluster.create.side_effect=create
        with patch.object(runtime.onebox_ctl.ctl,"check_local_docker"), \
             patch.object(runtime.onebox_ctl.ctl,"locked",side_effect=lock), \
             patch.object(runtime.onebox_ctl,"Onebox",return_value=cluster):
            owner=runtime.create_owned("owned")
        self.assertEqual(owner,"a"*32)
        self.assertEqual(cluster.state["owner"],"b"*32)

    def test_replaced_allocation_never_grants_new_owner_cleanup_authority(self):
        cluster=MagicMock()
        cluster.state={"owner":"b"*32}
        with patch("sys.argv",["rust_runtime_test.py","owned","--prebuilt","bundle"]), \
             patch.object(runtime,"create_owned",return_value="a"*32), \
             patch.object(runtime,"Allocation",side_effect=RuntimeError("replacement owner mismatch")) as allocation, \
             patch.object(runtime,"qualify") as qualify, \
             patch.object(runtime.onebox_ctl.ctl,"locked",return_value=contextlib.nullcontext(Path("/fake"))), \
             patch.object(runtime.onebox_ctl,"Onebox",return_value=cluster), \
             contextlib.redirect_stdout(io.StringIO()) as output:
            with self.assertRaises(SystemExit):
                runtime.main()
            allocation.assert_called_once_with("owned","a"*32)
            qualify.assert_not_called()
            cluster.destroy.assert_not_called()
            result=json.loads(output.getvalue())
            self.assertEqual(result["status"],"FAIL")
            self.assertIn("owner changed",result["errors"][1])


@unittest.skipUnless(sys.platform=="linux","Linux-only application-runtime harness")
class AllocationCommitTests(unittest.TestCase):
    def setUp(self):
        directory=tempfile.TemporaryDirectory(prefix=".allocation-test-",dir=".")
        self.addCleanup(directory.cleanup)
        self.root=Path(directory.name).resolve()
        self.absence=[]
        self.compose=[]
        self.original_owner=None
        image={"Id":"sha256:"+"c"*64,"Os":"linux","Architecture":"amd64",
               "Config":{"Labels":{runtime.onebox_ctl.Onebox.image_label:"1"}}}
        def docker(arguments,**_kwargs):
            if arguments[:3]==["docker","image","inspect"]:
                return json.dumps([image])
            if arguments[:2]==["docker","run"] or arguments[:3]==["docker","volume","rm"]:
                return ""
            if arguments[:3] in (["docker","ps","-aq"],["docker","volume","ls"],
                                 ["docker","network","ls"]):
                return ""
            raise AssertionError(f"unexpected Docker command: {arguments}")
        self.docker=docker
        def resources(cluster,**_kwargs):
            owner=cluster.state["owner"]
            return ([{"Id":"container-"+owner}],[{"Name":"volume-"+owner}],
                    [{"Id":"network-"+owner}])
        def inspect(arguments,**_kwargs):
            self.assertEqual(arguments[:1],["docker"])
            kind,_,identity=arguments[1:]
            self.absence.append((kind,identity))
            return subprocess.CompletedProcess(arguments,1,"",f"No such {kind}: {identity}")
        def readiness(cluster,*_args,**_kwargs):
            self.original_owner=json.loads(cluster.path.read_text())["owner"]
            raise TimeoutError("native readiness failed after allocation")
        for target,attribute,kwargs in (
            (runtime.onebox_ctl,"STATE_ROOT",{"new":self.root}),
            (runtime.onebox_ctl.ctl,"check_local_docker",{}),
            (runtime.onebox_ctl.ctl,"run",{"side_effect":docker}),
            (runtime.onebox_ctl.Onebox,"subnet_check",{}),
            (runtime.onebox_ctl.Onebox,"retain_image",{}),
            (runtime.onebox_ctl.Onebox,"image_tag_id",{"return_value":""}),
            (runtime.onebox_ctl.Onebox,"allocate",{"autospec":True,"side_effect":
                lambda cluster:cluster.save(phase="starting")}),
            (runtime.onebox_ctl.Onebox,"helper",{}),
            (runtime.onebox_ctl.Onebox,"resources",{"autospec":True,"side_effect":resources}),
            (runtime.onebox_ctl.Onebox,"compose",{"autospec":True,"side_effect":
                lambda _cluster,*args,**_kwargs:self.compose.append(args)}),
            (runtime.onebox_ctl.Onebox,"wait",{"autospec":True,"side_effect":readiness}),
            (runtime.subprocess,"run",{"side_effect":inspect}),
            (runtime,"load_bundle",{"return_value":model_binaries()}),
        ):
            mocked=patch.object(target,attribute,**kwargs)
            mocked.start()
            self.addCleanup(mocked.stop)

    def run_main(self):
        with patch("sys.argv",["rust_runtime_test.py","owned","--prebuilt","bundle"]), \
             contextlib.redirect_stdout(io.StringIO()) as output:
            with self.assertRaises(SystemExit):
                runtime.main()
        return json.loads(output.getvalue())

    def replace_owner(self):
        cluster=runtime.onebox_ctl.Onebox(self.root/"owned","owned")
        cluster.load()
        cluster.state["owner"]="b"*32
        compose,_=cluster.definition(cluster.state,cluster.directory)
        runtime.onebox_ctl.ctl.write_json(cluster.directory/"compose.json",compose)
        cluster.save()

    def workflow_command(self,name):
        workflow=(runtime.ROOT/".github/workflows/ci.yaml").read_text()
        step=workflow.split("- name: "+name+"\n",1)[1].split("\n    - name:",1)[0]
        shell=textwrap.dedent(step.split("      run:",1)[1].lstrip().removeprefix("|"))
        return shlex.split(shell)

    def test_readiness_failure_cleans_committed_owner_with_independent_absence(self):
        result=self.run_main()
        self.assertIn("readiness failed",result["errors"][0])
        cleanup=result["cleanup"]["onebox"]
        self.assertIsInstance(cleanup,dict,"newly committed allocation lost its cleanup capability")
        self.assertTrue(cleanup["absence_verified"])
        self.assertEqual(self.absence,[(kind,kind+"-"+self.original_owner)
                                      for kind in ("container","volume","network")])
        self.assertIn(("down","--timeout",str(runtime.onebox_ctl.ctl.SHUTDOWN_TIMEOUT)),self.compose)

    def test_committed_owner_survives_allocated_journal_failure_before_startup(self):
        original_event=runtime.onebox_ctl.Onebox.event
        def event(cluster,name,**values):
            if name=="allocated":
                self.original_owner=json.loads(cluster.path.read_text())["owner"]
                raise OSError("allocation journal failed")
            return original_event(cluster,name,**values)
        with patch.object(runtime.onebox_ctl.Onebox,"event",autospec=True,side_effect=event):
            result=self.run_main()
        self.assertIn("journal failed",result["errors"][0])
        self.assertIsInstance(result["cleanup"]["onebox"],dict)
        self.assertTrue(result["cleanup"]["onebox"]["absence_verified"])
        self.assertFalse(any(args[0]=="up" for args in self.compose))

    def test_rejected_existing_allocation_and_name_collision_grant_no_capability(self):
        for rejection in ("existing","collision"):
            with self.subTest(rejection=rejection):
                if rejection=="existing":
                    with patch.object(runtime.onebox_ctl.Onebox,"wait"):
                        runtime.create_owned("owned")
                def collision(arguments,**kwargs):
                    return "collision" if arguments[:3]==["docker","ps","-aq"] else self.docker(arguments,**kwargs)
                with patch.object(runtime.onebox_ctl.ctl,"run",
                                  side_effect=collision) if rejection=="collision" else contextlib.nullcontext(), \
                     patch.object(runtime,"destroy_owned") as destroy:
                    result=self.run_main()
                destroy.assert_not_called()
                self.assertIn("already exists" if rejection=="existing" else "refusing adoption",result["errors"][0])
                self.assertEqual(result["cleanup"]["onebox"],"create rejected; no cleanup capability")
                self.assertFalse(self.absence)
                if rejection=="existing":
                    cluster=runtime.onebox_ctl.Onebox(self.root/"owned","owned")
                    cluster.load()
                    with patch.object(cluster,"resources",return_value=([],[],[])):
                        cluster.destroy("owned")
                    # Keep the tombstone so collision rejection reaches Docker preflight.

    def run_ci_create(self):
        output_path=self.root/"github-output"
        command=self.workflow_command("Create native development cluster")
        with patch.dict(runtime.os.environ,{"CLUSTER_NAME":"owned","CLUSTER_CTL":"scripts/onebox_ctl.py",
                                            "GITHUB_OUTPUT":str(output_path)}), \
             patch("sys.argv",[runtime.os.path.expandvars(arg) for arg in command[1:]]), \
             patch.object(runtime.os,"umask"), \
             contextlib.redirect_stdout(io.StringIO()) as output, \
             contextlib.redirect_stderr(io.StringIO()) as error:
            status=runtime.onebox_ctl.main()
        return status,output_path,output.getvalue(),error.getvalue()

    def run_ci_failure(self,drift=False):
        status,output_path,output,error=self.run_ci_create()
        self.assertEqual(status,1)
        self.assertIn("readiness failed",error)
        self.assertTrue(output_path.exists(),"failed create skipped owner output")
        owner=output_path.read_text().strip().removeprefix("owner=")
        self.assertEqual(owner,self.original_owner)
        self.assertIn("::add-mask::"+owner,output)
        if drift:
            self.replace_owner()
        cleanup=self.workflow_command("Destroy only this job's cluster")[2]
        with patch.dict(runtime.os.environ,{"CLUSTER_NAME":"owned"}), \
             patch("sys.argv",["-c",owner]),contextlib.redirect_stdout(io.StringIO()):
            if drift:
                with self.assertRaisesRegex(RuntimeError,"owner changed"):
                    exec(compile(cleanup,"ci-cleanup","exec"),{})
            else:
                exec(compile(cleanup,"ci-cleanup","exec"),{})
        if drift:
            self.assertFalse(self.absence)
            self.assertFalse(any(args[0]=="down" for args in self.compose))
        else:
            self.assertEqual(self.absence,[(kind,kind+"-"+owner)
                                          for kind in ("container","volume","network")])

    def test_ci_failure_retains_original_output_and_owner_checked_teardown(self):
        self.run_ci_failure()

    def test_ci_failure_never_authorizes_replacement_owner(self):
        self.run_ci_failure(drift=True)

    def test_ci_rejected_existing_create_never_publishes_cleanup_owner(self):
        with patch.object(runtime.onebox_ctl.Onebox,"wait"):
            runtime.create_owned("owned")
        status,output_path,output,error=self.run_ci_create()
        self.assertEqual(status,1)
        self.assertIn("already exists",error)
        self.assertFalse(output_path.exists())
        self.assertNotIn("::add-mask::",output)

    def test_ci_name_collision_never_publishes_cleanup_owner(self):
        def collision(arguments,**kwargs):
            return "collision" if arguments[:3]==["docker","ps","-aq"] else self.docker(arguments,**kwargs)
        with patch.object(runtime.onebox_ctl.ctl,"run",side_effect=collision):
            status,output_path,output,error=self.run_ci_create()
        self.assertEqual(status,1)
        self.assertIn("refusing adoption",error)
        self.assertFalse(output_path.exists())
        self.assertNotIn("::add-mask::",output)

    def test_readiness_failure_never_recovers_replacement_owner(self):
        destroy=runtime.destroy_owned
        def replaced(name,owner):
            self.assertEqual(owner,self.original_owner)
            self.replace_owner()
            return destroy(name,owner)
        with patch.object(runtime,"destroy_owned",side_effect=replaced) as cleanup:
            result=self.run_main()
        cleanup.assert_called_once_with("owned",self.original_owner)
        self.assertIn("owner changed",result["errors"][1])
        self.assertFalse(self.absence)
        self.assertFalse(any(args[0]=="down" for args in self.compose))


@unittest.skipUnless(sys.platform=="linux","Linux-only application-runtime harness")
class AmbiguousUpgradeTests(unittest.TestCase):
    def execute_upgrade_timeout(self, drift=None):
        fixture=AllocationTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        state,container=fixture.state,fixture.container
        allocation=MagicMock()
        allocation.name="owned"; allocation.owner="capability"; allocation.container_id="immutable-container"
        def verify():
            return runtime.validate_allocation(state,container,"owned","capability")
        allocation.verify.side_effect=verify
        def execute(*_args,**_kwargs):
            verify()
            return "{}"
        allocation.execute.side_effect=execute
        present={"app":False,"versions":set(),"version":"1.1"}
        calls=[]
        application="fabric:/FubeRustRuntime012345abcdef"
        type_name="FubeRustRuntime012345abcdefType"
        def sf(*args,**_kwargs):
            verify()
            calls.append(args)
            if args==("application","list"):
                return json.dumps({"items":[{"name":application,"typeName":type_name,"typeVersion":present["version"]}] if present["app"] else []})
            if args==("application","type-list"):
                return json.dumps({"items":[{"name":type_name,"version":v} for v in present["versions"]]})
            if args[:2]==("application","provision"):
                present["versions"].add("1.1" if not present["versions"] else "1.2")
            elif args[:2]==("application","create"):
                present["app"]=True
            elif args[:2]==("application","upgrade"):
                present["version"]="9.9" if drift=="version" else "1.2"
                if drift=="owner":
                    state["owner"]="replacement-owner"
                    container["Config"]["Labels"][runtime.OWNER_LABEL]="replacement-owner"
                raise subprocess.TimeoutExpired("accepted upgrade with lost completion",1)
            elif args[:2]==("application","delete"):
                present["app"]=False
            elif args[:2]==("application","unprovision"):
                present["versions"].remove(args[-1])
            return "{}"
        allocation.sf.side_effect=sf
        observation={"rpc":{"partition":"partition","instance":1,"incarnation":2,"pid":3,"host":"host",
                           "packages":{"configuration":{"value":"before-upgrade"},"runtime_id":"runtime"}},
                     "native_entry_point_instance":7}
        def package(*_args,**kwargs):
            return Path("/generated-upgrade" if kwargs.get("upgrade") else "/generated"),type_name,"1.2" if kwargs.get("upgrade") else "1.1"
        binaries=model_binaries()
        with patch.object(runtime.uuid,"uuid4",return_value=MagicMock(hex="012345abcdef")), \
             patch.object(runtime,"package",side_effect=package), \
             patch.object(runtime,"run",return_value="native-free"), \
             patch.object(runtime,"digest",return_value="a"*64), \
             patch.object(runtime,"wait_observe",return_value=observation), \
             patch.object(runtime,"observe",return_value=observation), \
             patch.object(runtime,"deployment_artifacts",return_value={}), \
             patch.object(runtime,"restart") as restart, \
             patch.object(runtime,"destroy_owned") as destroy:
            result=runtime.qualify(allocation,binaries=binaries)
            restart.assert_not_called()
            destroy.assert_not_called()
        return result,present,calls

    def test_accepted_upgrade_timeout_reconciles_exact_two_owned_versions(self):
        result,present,calls=self.execute_upgrade_timeout()
        self.assertEqual(result["status"],"FAIL")
        self.assertIn("accepted upgrade",result["errors"][0])
        self.assertFalse(present["app"])
        self.assertFalse(present["versions"])
        self.assertEqual(result["owner"],"capability")
        self.assertEqual(set(result["cleanup"]["types"]),{"1.1","1.2"})
        self.assertEqual(sum(v[:2]==("application","upgrade") for v in calls),1)

    def test_ambiguous_upgrade_version_drift_refuses_deployment_cleanup(self):
        result,present,calls=self.execute_upgrade_timeout("version")
        self.assertTrue(present["app"])
        self.assertEqual(present["versions"],{"1.1","1.2"})
        self.assertIn("retained_for_recovery",result["cleanup"])
        self.assertFalse(any(v[:2] in {("application","delete"),("application","unprovision")} for v in calls))

    def test_ambiguous_upgrade_owner_drift_never_uses_replacement_capability(self):
        result,present,calls=self.execute_upgrade_timeout("owner")
        self.assertTrue(present["app"])
        self.assertEqual(result["owner"],"capability")
        self.assertIn("retained_for_recovery",result["cleanup"])
        self.assertTrue(any("ownership" in error for error in result["errors"]))
        self.assertFalse(any(v[:2] in {("application","delete"),("application","unprovision")} for v in calls))


@unittest.skipUnless(sys.platform=="linux","Linux-only application-runtime harness")
class VersionOwnershipTests(unittest.TestCase):
    def test_upgraded_application_and_each_owned_version_are_removed_and_verified(self):
        allocation=MagicMock()
        present={"app":True,"versions":{"1.1","1.2"}}
        calls=[]
        def sf(*args,**_kwargs):
            calls.append(args)
            if args==("application","list"):
                return json.dumps({"items":[{"name":"fabric:/Minted","typeName":"MintedType","typeVersion":"1.2"}] if present["app"] else []})
            if args==("application","type-list"):
                return json.dumps({"items":[{"name":"MintedType","version":v} for v in present["versions"]]})
            if args[:2]==("application","delete"): present["app"]=False
            if args[:2]==("application","unprovision"): present["versions"].remove(args[-1])
            return "{}"
        allocation.sf.side_effect=sf
        result={"application":"fabric:/Minted","type_name":"MintedType","version":"1.1","cleanup":{},"errors":[]}
        attempts={"create":True,"create_completed":True,"provision":True,"provision_completed":True,
                  "extra_versions":{"1.2":True}}
        runtime.cleanup_deployment(allocation,result,attempts)
        self.assertEqual(present,{"app":False,"versions":set()})
        self.assertFalse(result["errors"])
        self.assertEqual(set(result["cleanup"]["types"]),{"1.1","1.2"})
        self.assertEqual(sum(call==("application","type-list") for call in calls),4)
        self.assertLess(next(i for i,v in enumerate(calls) if v[:2]==("application","delete")),
                        next(i for i,v in enumerate(calls) if v[:2]==("application","unprovision")))

    def test_unowned_upgrade_version_drift_never_authorizes_cleanup(self):
        allocation=MagicMock()
        allocation.sf.return_value=json.dumps({"items":[{"name":"fabric:/Minted","typeName":"MintedType","typeVersion":"9.9"}]})
        result={"application":"fabric:/Minted","type_name":"MintedType","version":"1.1","cleanup":{},"errors":[]}
        runtime.cleanup_deployment(allocation,result,{"create":True,"create_completed":True,
            "provision":True,"provision_completed":True,"extra_versions":{"1.2":True}})
        self.assertTrue(result["errors"])
        self.assertEqual(allocation.sf.call_count,1)
        self.assertIn("retained_for_recovery",result["cleanup"])


@unittest.skipUnless(sys.platform=="linux","Linux-only application-runtime harness")
class AmbiguousMutationTests(unittest.TestCase):
    def qualify(self,lost,cleanup_failure=False):
        allocation=MagicMock(name="allocation")
        allocation.name="owned";allocation.owner="capability";allocation.container_id="cid"
        allocation.verify.return_value="cid"
        present={"type":False,"app":False}
        calls=[]
        type_name="FubeRustRuntimeabcdefgh1234Type"
        application="fabric:/FubeRustRuntimeabcdefgh1234"
        def sf(*args,**_kwargs):
            calls.append(args)
            if args==("application","list"):
                return json.dumps({"items":[{"name":application,"typeName":type_name,"typeVersion":"1.1"}] if present["app"] else []})
            if args==("application","type-list"):
                return json.dumps({"items":[{"name":type_name,"version":"1.1"}] if present["type"] else []})
            if args[:2]==("application","provision"):
                present["type"]=True
                if lost=="provision":raise subprocess.TimeoutExpired("accepted provision",1)
            if args[:2]==("application","create"):
                present["app"]=True
                if lost=="create":raise subprocess.TimeoutExpired("accepted create",1)
            if args[:2]==("application","delete"):
                if cleanup_failure:raise RuntimeError("cleanup delete failed")
                present["app"]=False
            if args[:2]==("application","unprovision"):present["type"]=False
            return "{}"
        allocation.sf.side_effect=sf
        with patch.object(runtime,"run",return_value="native-free"), \
             patch.object(runtime,"digest",return_value="a"*64), \
             patch.object(runtime.uuid,"uuid4",return_value=MagicMock(hex="abcdefgh1234")), \
             patch.object(runtime,"package",return_value=(Path("/generated"),type_name,"1.1")):
            result=runtime.qualify(allocation,binaries=model_binaries())
        return result,present,calls

    def test_accepted_provision_timeout_is_reconciled_and_removed(self):
        result,present,calls=self.qualify("provision")
        self.assertEqual(result["status"],"FAIL")
        self.assertFalse(present["type"])
        self.assertTrue(any(v[:2]==("application","unprovision") for v in calls))
        self.assertIn("accepted provision",result["errors"][0])

    def test_accepted_create_timeout_deletes_only_minted_app_then_type(self):
        result,present,calls=self.qualify("create")
        self.assertEqual(result["status"],"FAIL")
        self.assertEqual(present,{"type":False,"app":False})
        delete=next(i for i,v in enumerate(calls) if v[:2]==("application","delete"))
        unprovision=next(i for i,v in enumerate(calls) if v[:2]==("application","unprovision"))
        self.assertLess(delete,unprovision)
        self.assertIn("accepted create",result["errors"][0])

    def test_cleanup_error_preserves_original_failure_and_recovery_identity(self):
        result,present,_=self.qualify("create",cleanup_failure=True)
        self.assertEqual(result["status"],"FAIL")
        self.assertTrue(present["app"])
        self.assertIn("accepted create",result["errors"][0])
        self.assertIn("cleanup delete failed",result["errors"][1])
        self.assertEqual(result["cleanup"]["retained_for_recovery"]["application"],result["application"])

    def test_existing_owner_allocation_is_never_destroyed(self):
        with patch("sys.argv",["rust_runtime_test.py","owned","--existing-owner","capability","--prebuilt","bundle"]), \
             patch.object(runtime,"load_bundle",return_value=model_binaries()), \
             patch.object(runtime,"Allocation"),patch.object(runtime,"qualify",return_value={"status":"FAIL","errors":["failure"],"cleanup":{}}), \
             patch.object(runtime,"destroy_owned") as destroy,contextlib.redirect_stdout(io.StringIO()) as output:
            with self.assertRaises(SystemExit):runtime.main()
            destroy.assert_not_called()
            self.assertEqual(json.loads(output.getvalue())["cleanup"]["onebox"],"caller-owned allocation retained")


@unittest.skipUnless(sys.platform=="linux","Linux-only application-runtime harness")
class ProcessTests(unittest.TestCase):
    def fixture(self):
        return {"native_pid": 321, "native_entry_point": "/owned/app/rust-runtime-app",
                "rpc": {"host": "native-host", "service": "fabric:/Owned/Singleton", "code_sequence": 99}}

    def probe(self, expected, *, host="native-host", maps="/usr/lib/libc.so.6", stale=False):
        class FakePath:
            def __init__(self, value):
                self.value = value

            def __truediv__(self, value):
                return FakePath(self.value + "/" + str(value))

            def read_bytes(self):
                return (f"Fabric_ApplicationHostId={host}\0Fabric_ApplicationName=fabric:/Owned\0"
                        "Fabric_CodePackageInstanceSeqNum=99\0").encode()

            def read_text(self):
                if self.value.endswith("/stat"):
                    return "321 (rust-runtime-app) S 119 " + "0 " * 40
                return f"1-2 r-xp 0 0 0 {maps}"

            def open(self,mode):
                if mode != "rb" or not self.value.endswith("/exe"):
                    raise AssertionError("unexpected process file read")
                return io.BytesIO(b"model executable")

        with patch("sys.argv", ["probe", json.dumps(expected), "kill"]), \
             patch("pathlib.Path", FakePath), \
             patch("os.pidfd_open", side_effect=ProcessLookupError if stale else None, return_value=44), \
             patch("os.readlink", return_value="/owned/app/rust-runtime-app"), \
             patch("os.close"), patch("signal.pidfd_send_signal") as signal, patch("select.poll") as poll, \
             contextlib.redirect_stdout(io.StringIO()) as output:
            try:
                poll.return_value.poll.return_value=[(44,1)]
                exec(runtime.PROCESS, {})
            except (RuntimeError, ProcessLookupError):
                self.assertFalse(signal.called)
                return False
            self.assertEqual(signal.call_args.args[:2], (44, 9))
            self.assertEqual(json.loads(output.getvalue())["executable_sha256"],
                             runtime.hashlib.sha256(b"model executable").hexdigest())
            return True

    def test_pidfd_signal_is_bound_to_verified_native_identity(self):
        self.assertTrue(self.probe(self.fixture()))
        self.assertFalse(self.probe(self.fixture(), host="reused-pid-different-host"))
        self.assertFalse(self.probe(self.fixture(), stale=True))
        self.assertFalse(self.probe(self.fixture(), maps="/opt/libFabricRuntime.so"))
        self.assertFalse(self.probe(self.fixture(), maps="/opt/libFabricCommon.so"))
        changed = self.fixture()
        changed["native_entry_point"] = "/unrelated/rust-runtime-app"
        self.assertFalse(self.probe(changed))


if __name__ == "__main__":
    unittest.main()
