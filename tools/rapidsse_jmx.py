"""Read one of RapidsSE's own JMeter test plans (bamboo/perftest/configs/tests/
*.jmx, e.g. tpch-sf1-2D2F.jmx, the TPC-H SF1 test of its daily regression job)
into the flat list of steps JMeter would execute, so the test can run without
JMeter itself: the same SQL, in the same order, the same loop counts, the
same assertions, the same stop-on-error rule.

What is taken from the plan (everything that decides what runs):
  - enabled="false" on any element: the element and all it contains are skipped
  - User Defined Variables (TestPlan / Arguments): ${NAME} substituted
    (${__P(NAME,default)} -> the -J override or the default)
  - LoopController loops: its children repeated
  - JDBCSampler: query, queryType (Select / Update Statement), variableNames
  - ResponseAssertion (scope variable, EQUALS / CONTAINS / SUBSTRING): attached
    to the sampler that sets the variable
  - ThreadGroup on_sample_error=stoptest: the run stops at the first failure
  - PostThreadGroup (tearDown): run after the main group, failures ignored

What has no effect on the database and is reported but not executed: log-only
JSR223 samplers, result writers/savers, BeanShell listeners. SystemSampler
(data generation) is reported: the runner generates the data itself, the same
way (tools/rapidsse_tpch.py). Anything else enabled raises, rather than being
silently dropped.

Path remapping (`remap`): the plans hard-code the data locations of the
machine they were written for (/mnt/disk1/data1/...); the caller passes a
prefix map so the same plan reads the same layout somewhere else.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

# JMeter ResponseAssertion test_type bits
_AT_CONTAINS, _AT_MATCH, _AT_EQUALS, _AT_SUBSTRING, _AT_NOT = 2, 1, 8, 16, 4

# elements that only log or record results: no effect on the database
_PASSIVE = {"ResultCollector", "ResultSaver", "BeanShellListener", "JSR223Sampler", "BeanShellPreProcessor",
            "com.rapidsdata.jmeter.samplers.JdbcRecorder", "ConstantTimer", "JDBCDataSource", "Arguments"}


@dataclass
class Assertion:
    variable: str
    expected: str
    kind: str  # equals | contains | substring
    negate: bool = False

    def check(self, value: str | None) -> bool:
        v = "" if value is None else str(value).strip()
        if self.kind == "equals":
            ok = v == self.expected
        else:
            ok = self.expected in v
        return ok != self.negate


@dataclass
class Step:
    name: str
    sql: str
    select: bool
    group: str  # main | teardown
    path: str  # where in the plan (for the report)
    variable: str = ""  # variableNames: first column of the first row lands in <variable>_1
    assertions: list[Assertion] = field(default_factory=list)


@dataclass
class Plan:
    source: str
    variables: dict
    steps: list[Step]
    skipped: list[str]  # enabled elements reported but not executed (log/record only, data generation)
    disabled: list[str]  # elements switched off in the plan (with what they contain)


def _props(el: ET.Element) -> dict:
    out = {}
    for p in el:
        if p.tag.endswith("Prop") and p.get("name") is not None:
            out[p.get("name")] = p.text or ""
    return out


def _children(tree: ET.Element):
    """A JMeter hashTree: element, its own hashTree (children), element, ..."""
    kids = list(tree)
    i = 0
    while i < len(kids):
        el = kids[i]
        sub = kids[i + 1] if i + 1 < len(kids) and kids[i + 1].tag == "hashTree" else None
        yield el, sub
        i += 2 if sub is not None else 1


def _subst(text: str, variables: dict, overrides: dict) -> str:
    def prop(m):
        name, default = m.group(1), m.group(2)
        return str(overrides.get(name, default))
    text = re.sub(r"\$\{__P\(([^,)]+),([^)]*)\)\}", prop, text)
    return re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", lambda m: str(variables.get(m.group(1), m.group(0))), text)


def _arguments(el: ET.Element) -> dict:
    out = {}
    for ep in el.iter("elementProp"):
        if ep.get("elementType") == "Argument":
            p = _props(ep)
            if p.get("Argument.name"):
                out[p["Argument.name"]] = p.get("Argument.value", "")
    return out


def parse(path: str, *, overrides: dict | None = None, remap: dict | None = None) -> Plan:
    overrides = dict(overrides or {})
    root = ET.parse(path).getroot()
    variables: dict = {}
    steps: list[Step] = []
    skipped: list[str] = []
    disabled: list[str] = []
    top = root.find("hashTree")

    def apply_remap(sql: str) -> str:
        for old, new in (remap or {}).items():
            sql = sql.replace(old, new)
        return sql

    # variables first (JMeter evaluates the test plan's arguments before running)
    for el, sub in _children(top):
        if el.tag == "TestPlan":
            for ep in el.iter("elementProp"):
                if ep.get("name") == "TestPlan.user_defined_variables":
                    variables.update(_arguments(ep))
            for el2, _ in _children(sub):
                if el2.tag == "Arguments" and el2.get("enabled", "true") != "false":
                    variables.update(_arguments(el2))
    for k in list(variables):
        variables[k] = _subst(variables[k], variables, overrides)
    variables.update({k: v for k, v in overrides.items() if k in variables})

    def walk(tree, where: str, group: str, last_sampler: list):
        for el, sub in _children(tree):
            name = el.get("testname") or el.tag
            here = f"{where}/{name}"
            if el.get("enabled", "true") == "false":
                disabled.append(here)
                continue
            tag = el.tag
            p = _props(el)
            if tag == "JDBCSampler":
                sql = apply_remap(_subst(p.get("query", ""), variables, overrides))
                st = Step(name=name, sql=sql, select=p.get("queryType", "").startswith("Select"), group=group,
                          path=here, variable=(p.get("variableNames") or "").split(",")[0].strip())
                steps.append(st)
                last_sampler[:] = [st]
                if sub is not None:
                    walk(sub, here, group, last_sampler)
            elif tag == "ResponseAssertion":
                st = last_sampler[0] if last_sampler else None
                tt = int(p.get("Assertion.test_type", "8") or 8)
                kind = "equals" if tt & _AT_EQUALS else "substring" if tt & _AT_SUBSTRING else "contains"
                expected = [c.text or "" for c in el.iter("stringProp") if c.get("name") not in (
                    "Assertion.test_field", "Assertion.scope", "Scope.variable", "Assertion.custom_message",
                    "Assertion.assume_success", "Assertion.test_type") and c.get("name", "").lstrip("-").isdigit()]
                var = p.get("Scope.variable", "") if p.get("Assertion.scope") == "variable" else ""
                target = None
                if var:
                    # the sampler whose variableNames sets this variable (<name>_1)
                    base = var.rsplit("_", 1)[0]
                    target = next((s for s in reversed(steps) if s.variable == base), None)
                target = target or st
                if target is None:
                    raise ValueError(f"{here}: assertion with no sampler to attach to")
                for e in expected:
                    target.assertions.append(Assertion(var or target.variable, _subst(e, variables, overrides), kind,
                                                       bool(tt & _AT_NOT)))
            elif tag in ("ThreadGroup", "SetupThreadGroup", "PostThreadGroup"):
                if tag == "ThreadGroup" and p.get("ThreadGroup.num_threads", "1") != "1":
                    raise ValueError(f"{here}: {p['ThreadGroup.num_threads']} threads -- only 1 is supported")
                g = "teardown" if tag == "PostThreadGroup" else "main"
                loops = int(_subst(p.get("LoopController.loops", "1") or "1", variables, overrides))
                for _ in range(max(loops, 1)):
                    walk(sub, here, g, [])
            elif tag == "LoopController":
                loops = int(_subst(p.get("LoopController.loops", "1") or "1", variables, overrides))
                for _ in range(loops):
                    walk(sub, here, group, last_sampler)
            elif tag in ("GenericController", "TransactionController"):
                walk(sub, here, group, last_sampler)
            elif tag == "IfController":
                cond = p.get("IfController.condition", "")
                # every IfController in these plans tests the last sample's
                # failure; its children stop the test. The runner stops on
                # the first failure itself (on_sample_error=stoptest).
                if "last_sample_ok" not in cond and "HAS_FAILED_QUERY" not in cond:
                    raise ValueError(f"{here}: IfController condition not understood: {cond!r}")
                skipped.append(f"{here} (stop on failure: the runner stops at the first failed step)")
            elif tag == "SystemSampler":
                skipped.append(f"{here} (data generation: done by the runner, same dbgen chunking)")
            elif tag == "com.rapidsdata.jmeter.samplers.SethPlugin":
                raise ValueError(f"{here}: SethPlugin script steps are not supported")
            elif tag == "BeanShellSampler":
                q = p.get("BeanShellSampler.query", "")
                if "System.exit" in q or "log.info" in q:
                    skipped.append(f"{here} (log / exit-on-failure)")
                else:
                    raise ValueError(f"{here}: BeanShell script not understood: {q[:80]!r}")
            elif tag in _PASSIVE or tag.endswith("Collector"):
                if tag == "JSR223Sampler":
                    skipped.append(f"{here} (log only)")
                if sub is not None and tag not in ("ResultCollector",):
                    walk(sub, here, group, last_sampler)
            elif tag in ("TestPlan",):
                walk(sub, here, group, last_sampler)
            else:
                raise ValueError(f"{here}: plan element {tag} not supported")

    walk(top, "", "main", [])
    # teardown last, as JMeter runs it after the thread groups
    steps = [s for s in steps if s.group == "main"] + [s for s in steps if s.group == "teardown"]
    return Plan(source=path, variables=variables, steps=steps, skipped=skipped, disabled=disabled)
