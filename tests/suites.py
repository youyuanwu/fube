"""Strict, shared suite selection and expected coverage; no shell commands."""

import hashlib
from pathlib import PurePosixPath
import re
import tomllib

from tests import ROOT

PATH = ROOT / "tests/suites.toml"
FIELDS = {
    "unittest": {"start", "pattern"},
    "nextest": {"filter", "ignored", "reports"},
    "data": {"profile", "report"},
    "legacy": {"report"},
    "native-runtime": {"action"},
}


def require(condition, message):
    if not condition:
        raise ValueError("suite manifest: " + message)


def names(value):
    return (type(value) is list and bool(value)
            and all(type(item) is str and re.fullmatch(r"[a-z][a-z0-9_-]*", item) for item in value)
            and len(value) == len(set(value)))


def relative(value):
    return (type(value) is str and bool(value) and re.fullmatch(r"[a-zA-Z0-9_./-]+", value)
            and not PurePosixPath(value).is_absolute()
            and all(part not in ("", ".", "..") for part in value.split("/")))


class Manifest:
    def __init__(self, value):
        require(type(value) is dict and set(value) == {
            "version", "scenario_groups", "profiles", "suites", "partitions"}, "root keys")
        require(type(value["version"]) is int and value["version"] == 1, "unsupported version")
        self.groups, self.profiles = value["scenario_groups"], value["profiles"]
        self.suites, self.partitions = value["suites"], value["partitions"]
        for label, table in (("scenario groups", self.groups), ("profiles", self.profiles),
                             ("suites", self.suites), ("partitions", self.partitions)):
            require(type(table) is dict and names(list(table)), label + " names")
        require(set(self.groups) == {"common", "mixed", "legacy"}
                and set(self.profiles) == {"rust", "mixed", "full", "native-data"},
                "implemented scenario groups/profiles")
        for group in self.groups.values():
            require(names(group), "nonempty unique scenario names")
        all_scenarios = [name for group in self.groups.values() for name in group]
        require(len(all_scenarios) == len(set(all_scenarios)), "duplicate scenario across groups")
        for groups in self.profiles.values():
            require(names(groups) and set(groups) <= self.groups.keys(), "unknown/duplicate profile group")
        reports = []
        for name, suite in self.suites.items():
            require(type(suite) is dict and type(suite.get("kind")) is str
                    and suite["kind"] in FIELDS, name + ": unknown runner")
            kind = suite["kind"]
            require(set(suite) == {"kind", "resource", "timeout"} | FIELDS[kind], name + ": runner fields")
            require(type(suite["timeout"]) is int and 0 < suite["timeout"] <= 10800, name + ": timeout")
            resource = suite["resource"]
            require(resource in ("none", "onebox", "native-runtime"), name + ": resource")
            if kind == "unittest":
                require(resource == "none" and relative(suite["start"])
                        and (suite["start"] == "tests/offline" or suite["start"].startswith("tests/offline/"))
                        and (ROOT / suite["start"]).is_dir(), name + ": discovery directory")
                require(type(suite["pattern"]) is str and suite["pattern"].startswith("test_")
                        and "/" not in suite["pattern"] and "\\" not in suite["pattern"], name + ": pattern")
            elif kind == "nextest":
                require(resource in ("none", "onebox") and type(suite["filter"]) is str
                        and bool(suite["filter"]) and type(suite["ignored"]) is bool, name + ": nextest selection")
                require(type(suite["reports"]) is list
                        and all(relative(path) and path.endswith("-report.json") for path in suite["reports"]),
                        name + ": report paths")
                require((resource == "onebox" and len(suite["reports"]) == 5 and suite["ignored"])
                        or (resource == "none" and suite["reports"] == [] and not suite["ignored"]),
                        name + ": native report/ignored policy")
                reports.extend(suite["reports"])
            elif kind in ("data", "legacy"):
                require(resource == "onebox" and relative(suite["report"])
                        and suite["report"].endswith("-report.json"), name + ": report/resource")
                reports.append(suite["report"])
                if kind == "data":
                    require(suite["profile"] in ("rust", "native-data"),
                            name + ": data profile unsupported by the strict CI validator")
            else:
                require(resource == "native-runtime" and suite["action"] in (
                    "bootstrap", "close", "abort", "manifest-close", "manifest-abort"), name + ": native action")
        require(sum(suite["kind"] == "nextest" and suite["resource"] == "onebox"
                    for suite in self.suites.values()) == 1, "exactly one native-client report suite is supported")
        require(len(reports) == len(set(reports)), "duplicate suite report path")
        require(not set(reports) & {"cleanup.json", "validated-input.json", "worker-proof.json",
                                   "suite-outcomes.json", "diagnostics.json"}, "reserved report path")
        assigned, ids = [], []
        for name, partition in self.partitions.items():
            require(type(partition) is dict and set(partition) == {"id", "timeout_minutes", "suites"},
                    name + ": partition fields")
            require(type(partition["id"]) is str and re.fullmatch(r"[a-z]{1,4}", partition["id"]),
                    name + ": allocation suffix")
            require(type(partition["timeout_minutes"]) is int and 0 < partition["timeout_minutes"] <= 360,
                    name + ": worker timeout")
            require(names(partition["suites"]) and set(partition["suites"]) <= self.suites.keys(),
                    name + ": unknown/duplicate suite")
            require(all(self.suites[suite]["resource"] == "onebox" for suite in partition["suites"]),
                    name + ": only onebox suites may share an allocation")
            assigned.extend(partition["suites"])
            ids.append(partition["id"])
        require(len(ids) == len(set(ids)) and len(assigned) == len(set(assigned)), "duplicate partition assignment")
        require(set(assigned) == {name for name, suite in self.suites.items() if suite["resource"] == "onebox"},
                "each onebox suite must be assigned exactly once")
        self.coverage()

    def scenarios(self, profile):
        require(profile in self.profiles, "unknown profile: " + profile)
        return tuple(name for group in self.profiles[profile] for name in self.groups[group])

    def selected(self, partition):
        require(partition in self.partitions, "unknown partition: " + partition)
        return {name: self.suites[name] for name in self.partitions[partition]["suites"]}

    def matrix(self):
        return {"include": [{"route": name, "id": partition["id"],
                             "timeout": partition["timeout_minutes"],
                             "archive": any(suite["kind"] == "nextest" for suite in self.selected(name).values())}
                            for name, partition in self.partitions.items()]}

    def coverage(self):
        assertions, native = [], []
        for partition in self.partitions:
            for suite in self.selected(partition).values():
                if suite["kind"] == "data":
                    assertions.extend(suite["profile"] + "/" + name for name in self.scenarios(suite["profile"]))
                elif suite["kind"] == "legacy":
                    assertions.extend("legacy/" + name for name in self.groups["legacy"])
                else:
                    native.extend("native-client/" + PurePosixPath(path).name.removesuffix("-report.json")
                                  for path in suite["reports"])
        require(len(assertions) == len(set(assertions)) and len(native) == len(set(native)),
                "duplicate expected assertion")
        return assertions, native


def load():
    contents = PATH.read_bytes()
    require(len(contents) <= 65536, "file exceeds 64 KiB")
    return Manifest(tomllib.loads(contents.decode("utf-8")))


def digest():
    return hashlib.sha256(PATH.read_bytes()).hexdigest()


MANIFEST = load()
