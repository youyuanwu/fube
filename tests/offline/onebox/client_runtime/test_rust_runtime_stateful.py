"""Offline evidence/ownership checks; these are not native qualification."""
import copy
import contextlib
import io
from pathlib import Path
import subprocess
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from tests.onebox.shared import runtime


def snapshot():
    epoch={"data_loss":1,"configuration":(1<<32)+1}
    service="fabric:/Owned/Stateful"
    rpc={"service":service,"partition":"partition","replica":7,"incarnation":8,"node":"node","host":"host",
         "pid":11,"role":"Primary","replicator_role":"Primary","current_role":"Primary",
         "stateful":True,"persisted":False,"epoch":epoch,"service_address":"http://127.0.0.1:1/service",
         "replication_address":"http://127.0.0.1:2/control","read_status":"Granted","write_status":"Granted",
         "replicator":{"application_writes":False,"durability":False}}
    return {"observer":"fube-client","service":service,"partition":"partition",
            "native_partition":{"target":1,"minimum":1,"status":"Ready","epoch":{"data_loss":1,"configuration":1<<32}},
            "replicas":[{"replica":7,"role":"Primary","status":"Ready","node":"node","address":rpc["service_address"],
                         "service_http_status":200,"rpc":rpc,
                         "deployed":{"host_process_id":11,"service_type_name":"RustStatefulRuntimeType"},
                         "control":{"version":1,"state":"empty","partition":"partition","replica":7,"incarnation":8,
                                    "first":0,"last":0,"role":"Primary",
                                    "epoch":{"data_loss":1,"configuration":1<<32}}}],
            "primary":{"resolver":"fube-client","rpc":copy.deepcopy(rpc),"native_pid":11,
                       "endpoint":rpc["service_address"]}}


class StatefulEvidenceTests(unittest.TestCase):
    def test_three_requires_full_before_allocation_and_full_dispatches_preserved_tier(self):
        from unittest.mock import Mock
        allocation=SimpleNamespace(name="owned",owner="original",container_id="id",verify=lambda:"id",sf=Mock(return_value="[]"))
        with self.assertRaises(ValueError):
            runtime.qualify(allocation,False,stateful="three")
        allocation.sf.assert_not_called()
        with patch("sys.argv",["runtime","owned","--stateful","three","--prebuilt","bundle"]), \
             patch.object(runtime,"create_owned") as create,patch.object(runtime,"load_bundle") as load, \
             contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):runtime.main()
            create.assert_not_called();load.assert_not_called()
        binaries=SimpleNamespace(app=Path("app"),observer=Path("observer"),template_root=Path("template"),
            dependency_graph="graph",bundle_manifest_sha256="hash",source_revision="a"*40)
        with contextlib.ExitStack() as stack:
            for name in ("stateful_singleton","configuration_upgrade","stateful_three","cleanup_deployment"):
                stack.enter_context(patch.object(runtime,name))
            stack.enter_context(patch.object(runtime,"run",return_value=""))
            stack.enter_context(patch.object(runtime,"digest",return_value="hash"))
            stack.enter_context(patch.object(runtime,"package",return_value=(Path("package"),"OwnedType","1.1")))
            stack.enter_context(patch.object(runtime,"wait_observe",return_value={"rpc":{"packages":{"generation":1}}}))
            stack.enter_context(patch.object(runtime,"restart",return_value={}))
            stack.enter_context(patch.object(runtime,"wait_stateful",return_value={}))
            controls=stack.enter_context(patch.object(runtime,"controls_and_authority"))
            result=runtime.qualify(allocation,True,binaries,stateful="three")
            controls.assert_called_once()
            self.assertIn("resilience",result)
            self.assertEqual(result["status"],"PASS_FULL_EMPTY_STATEFUL_CONTROL_PLANE_AND_STATELESS_PROFILE")

    def test_native_departure_requires_independent_inventory_not_route_counters(self):
        import json
        value={"observer":"fube-client","node":"N0010","service":"fabric:/Owned/Stateful",
               "fixture_contacted":False,"deployed":[{"replica_id":7}]}
        def once(check,*_):return check()
        with patch.object(runtime,"wait_condition",side_effect=once),patch.object(runtime,"run",side_effect=lambda *_args,**_kwargs:json.dumps(value)):
            self.assertFalse(runtime.wait_native_replica_absent(Path("observer"),value["service"],value["node"],7))
            value["deployed"]=[]
            self.assertEqual(runtime.wait_native_replica_absent(Path("observer"),value["service"],value["node"],7),value)
            for mutation in [dict(observer="fixture"),dict(fixture_contacted=True),dict(deployed=[{"replica_id":"7"}])]:
                bad={**value,**mutation}
                with patch.object(runtime,"run",return_value=json.dumps(bad)):
                    with self.assertRaises(RuntimeError):
                        runtime.wait_native_replica_absent(Path("observer"),value["service"],value["node"],7)

    def test_three_replica_proof_requires_distinct_native_placements_and_secondary_route_denial(self):
        value=snapshot()
        value["native_partition"].update(target=3,minimum=2)
        for node,replica,pid in [("node2",8,12),("node3",9,13)]:
            secondary=copy.deepcopy(value["replicas"][0])
            secondary.update(node=node,replica=replica,role="ActiveSecondary",address=f"http://127.0.0.1:{pid}/service",service_http_status=503)
            secondary["rpc"].update(node=node,replica=replica,pid=pid,host=node,
                role="Secondary",replicator_role="Secondary",current_role="Secondary",
                service_address=secondary["address"],replication_address=f"http://127.0.0.1:{pid+100}/control",
                read_status="NotPrimary",write_status="NotPrimary")
            secondary["deployed"]["host_process_id"]=pid
            secondary["control"].update(replica=replica,role="Secondary")
            value["replicas"].append(secondary)
        runtime.assert_stateful(value,value["service"],3,2)
        for mutate in [
            lambda v:v["replicas"][1].update(node="node"),
            lambda v:v["replicas"][1].update(service_http_status=200),
            lambda v:v["replicas"][1]["rpc"].update(read_status="Granted"),
        ]:
            bad=copy.deepcopy(value);mutate(bad)
            with self.assertRaises(RuntimeError):runtime.assert_stateful(bad,value["service"],3,2)

    def test_native_discovery_placement_deployment_http_and_control_must_all_agree(self):
        value=snapshot()
        self.assertIs(runtime.assert_stateful(value,value["service"],1,1),value)
        mutations=[
            lambda v:v.update(observer="fixture"),
            lambda v:v["native_partition"].update(target=3),
            lambda v:v["replicas"][0].update(role="ActiveSecondary"),
            lambda v:v["replicas"][0]["rpc"].update(pid=99),
            lambda v:v["replicas"][0]["rpc"].update(incarnation=9),
            lambda v:v["replicas"][0]["rpc"].update(read_status="ReconfigurationPending"),
            lambda v:v["replicas"][0]["rpc"].update(replication_address=v["replicas"][0]["address"]),
            lambda v:v["replicas"][0]["rpc"].update(epoch={"data_loss":2,"configuration":(1<<32)+1}),
            lambda v:v["replicas"][0]["control"].update(last=1),
            lambda v:v["replicas"][0]["control"].update(epoch={"data_loss":1,"configuration":2<<32}),
            lambda v:v["replicas"][0]["rpc"]["replicator"].update(durability=True),
        ]
        for mutate in mutations:
            changed=copy.deepcopy(value); mutate(changed)
            with self.assertRaises(RuntimeError):
                runtime.assert_stateful(changed,value["service"],1,1)

    def test_missing_prebuilt_is_rejected_without_cargo_or_resource_mutation(self):
        with patch("sys.argv",["rust_runtime_test.py","owned","--stateful","one"]), \
             patch.object(runtime,"built_binaries") as build, patch.object(runtime,"create_owned") as create, \
             contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):runtime.main()
            build.assert_not_called();create.assert_not_called()


class ResourceAbsenceTests(unittest.TestCase):
    def test_resource_specific_absence_wordings_not_daemon_errors(self):
        for kind,error in [
            ("container","Error: No such container: original"),
            ("container","Error: No such object: original"),
            ("volume","Error response from daemon: get original: no such volume"),
            ("network","Error response from daemon: network original not found"),
            ("network","Error: No such network: original"),
        ]:
            with patch.object(runtime.subprocess,"run",return_value=subprocess.CompletedProcess([],1,"",error)):
                runtime.require_resource_absent(kind,"original")
        for error in ["permission denied","Cannot connect to Docker daemon","no such volume: other"]:
            with patch.object(runtime.subprocess,"run",return_value=subprocess.CompletedProcess([],1,"",error)):
                with self.assertRaises(RuntimeError):runtime.require_resource_absent("volume","original")
        with patch.object(runtime.subprocess,"run",return_value=subprocess.CompletedProcess([],0,"[]","")):
            with self.assertRaises(RuntimeError):runtime.require_resource_absent("container","original")
