"""Offline evidence/ownership checks; these are not native qualification."""
import copy
import contextlib
import io
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch
import rust_runtime_test as runtime


def snapshot():
    epoch={"data_loss":1,"configuration":(1<<32)+1}
    service="fabric:/Owned/Stateful"
    rpc={"service":service,"partition":"partition","replica":7,"incarnation":8,"node":"node","host":"host",
         "pid":11,"role":"Primary","replicator_role":"Primary","current_role":"Primary",
         "stateful":True,"persisted":False,"epoch":epoch,"service_address":"http://127.0.0.1:1/service",
         "replication_address":"http://127.0.0.1:2/control","read_status":"Granted","write_status":"Granted",
         "replicator":{"application_writes":False,"durability":False}}
    return {"observer":"mssf-client","service":service,"partition":"partition",
            "native_partition":{"target":1,"minimum":1,"status":"Ready","epoch":{"data_loss":1,"configuration":1<<32}},
            "replicas":[{"replica":7,"role":"Primary","status":"Ready","node":"node","address":rpc["service_address"],
                         "service_http_status":200,"rpc":rpc,
                         "deployed":{"host_process_id":11,"service_type_name":"RustStatefulRuntimeType"},
                         "control":{"version":1,"state":"empty","partition":"partition","replica":7,"incarnation":8,
                                    "first":0,"last":0,"role":"Primary",
                                    "epoch":{"data_loss":1,"configuration":1<<32}}}],
            "primary":{"resolver":"mssf-client","rpc":copy.deepcopy(rpc),"native_pid":11,
                       "endpoint":rpc["service_address"]}}


class StatefulEvidenceTests(unittest.TestCase):
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

    @unittest.skipUnless(sys.platform=="linux","native CLI is Linux-only")
    def test_missing_prebuilt_is_rejected_without_cargo_or_resource_mutation(self):
        with patch("sys.argv",["rust_runtime_test.py","owned","--stateful","one"]), \
             patch.object(runtime,"build_binaries") as build, patch.object(runtime,"create_owned") as create, \
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
