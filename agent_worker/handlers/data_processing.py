"""
Data Processing Handler — Preset Transform Cards + Global Variables.
New format (transforms) + Legacy format (operations) support.
"""
from __future__ import annotations
import copy, logging
from typing import Any, Dict, List
from simpleeval import simple_eval
from shared.models import TaskRequest, TaskResult
from ..registry import register
from .base import TaskHandler

log = logging.getLogger("worker.data_processing")

def _eval_expr(expr: str, ctx: Dict[str, Any]) -> Any:
    _functions = {
        'len': len, 'str': str, 'int': int, 'float': float,
        'list': list, 'dict': dict, 'min': min, 'max': max,
        'sum': sum, 'sorted': sorted, 'abs': abs, 'round': round,
        'filter': filter, 'map': map, 'zip': zip, 'enumerate': enumerate,
        'range': range, 'True': True, 'False': False, 'None': None,
        'pluck': lambda items, k: [i[k] for i in items] if items else [],
        'where': lambda items, k, v: [i for i in items if i.get(k) == v],
    }
    try:
        return simple_eval(expr, names=ctx, functions=_functions)
    except Exception as e:
        raise ValueError(f"Expr failed: {expr!r} -> {e}")

_COND_OPS = {
    "eq": lambda a, b: a == b,
    "neq": lambda a, b: a != b,
    "gt": lambda a, b: a is not None and float(a) > float(b),
    "gte": lambda a, b: a is not None and float(a) >= float(b),
    "lt": lambda a, b: a is not None and float(a) < float(b),
    "lte": lambda a, b: a is not None and float(a) <= float(b),
    "contains": lambda a, b: str(b) in str(a or ""),
    "not_contains": lambda a, b: str(b) not in str(a or ""),
    "is_empty": lambda a, b: not a,
    "not_empty": lambda a, b: bool(a),
    "starts_with": lambda a, b: str(a or "").startswith(str(b)),
    "ends_with": lambda a, b: str(a or "").endswith(str(b)),
}

def _t_select(data, op, ctx):
    fields = op.get("fields", {})
    if isinstance(data, dict):
        # Single dict: return plain dict (not list) — enables merging downstream
        return {k: _eval_expr(v, {**ctx, "item": data}) for k, v in fields.items()}
    if isinstance(data, list):
        return [{k: _eval_expr(v, {**ctx, "item": item}) for k, v in fields.items()} for item in data]
    raise ValueError(f"select source must be array or dict, got {type(data).__name__}")

def _t_filter(data, op, ctx):
    conds = op.get("conditions", [])
    if not isinstance(data, list): raise ValueError(f"filter source must be array")
    result = []
    for item in data:
        ok = True
        for c in conds:
            fn = _COND_OPS.get(c.get("op", "eq"))
            if not fn: raise ValueError(f"Unknown op: {c.get('op')}")
            iv = item.get(c["field"]) if isinstance(item, dict) else None
            if not fn(iv, c.get("val")): ok = False; break
        if ok: result.append(item)
    return result

def _t_sort(data, op, ctx):
    key = op.get("key", ""); rev = op.get("reverse", False)
    if not isinstance(data, list): raise ValueError(f"sort source must be array")
    return sorted(data, key=lambda x: x.get(key) if isinstance(x, dict) else x, reverse=rev)

def _t_compute(data, op, ctx):
    return _eval_expr(op.get("expression", ""), {**ctx, "source": data})

def _t_merge(data, op, ctx):
    merged = {}
    for src in op.get("sources", []):
        v = ctx.get(src)
        if isinstance(v, dict): merged.update(v)
    return merged

_TRANSFORMS = {"select": _t_select, "filter": _t_filter, "sort": _t_sort, "compute": _t_compute, "merge": _t_merge}

# Legacy ops
def _lg_get(data, path):
    if not path or path == "$": return data
    cur = data
    for k in path.lstrip("$.").split("."):
        if isinstance(cur, dict): cur = cur.get(k)
        elif isinstance(cur, (list, tuple)):
            try: cur = cur[int(k)]
            except: return None
        else: return None
    return cur
def _lg_set(data, path, val):
    if path == "$" or not path: return
    keys = path.lstrip("$.").split(".")
    cur = data
    for i, k in enumerate(keys[:-1]):
        if k not in cur or not isinstance(cur[k], dict): cur[k] = {}
        cur = cur[k]
    cur[keys[-1]] = val
def _lg_map(data, op, ctx):
    src = _lg_get(data, op.get("source", "$")); tgt = op.get("target", "$")
    fields = op.get("fields", {})
    if not isinstance(src, list): raise ValueError(f"map source must be array")
    result = [{k: _eval_expr(v, {**ctx, "item": item}) for k, v in fields.items()} if fields else _eval_expr(op.get("expression", "item"), {**ctx, "item": item}) for item in src]
    _lg_set(data, tgt, result); return data
def _lg_filter(data, op, ctx):
    src = _lg_get(data, op.get("source", "$")); tgt = op.get("target", "$")
    cond = op.get("condition", "True")
    if not isinstance(src, list): raise ValueError(f"filter source must be array")
    _lg_set(data, tgt, [item for item in src if _eval_expr(cond, {**ctx, "item": item})]); return data
def _lg_merge(data, op, ctx):
    tgt = op.get("target", "$")
    merged = {}
    for s in op.get("sources", []):
        v = _lg_get(data, s)
        if isinstance(v, dict): merged.update(v)
    _lg_set(data, tgt, merged); return data
def _lg_compute(data, op, ctx):
    _lg_set(data, op.get("target", "$.result"), _eval_expr(op.get("expression", ""), ctx)); return data
def _lg_sort(data, op, ctx):
    src = _lg_get(data, op.get("source", "$")); tgt = op.get("target", "$")
    if not isinstance(src, list): raise ValueError(f"sort source must be array")
    _lg_set(data, tgt, sorted(src, key=lambda x: _eval_expr(op.get("expression", "item"), {**ctx, "item": x}), reverse=op.get("reverse", False))); return data
_LEGACY = {"map": _lg_map, "filter": _lg_filter, "merge": _lg_merge, "compute": _lg_compute, "sort": _lg_sort}

@register("data_processing")
class DataProcessingHandler(TaskHandler):
    async def handle(self, req: TaskRequest) -> TaskResult:
        payload = req.payload
        config = payload.get("transform", {})
        on_error = config.get("on_error", "fail")
        transforms = config.get("transforms", None)
        operations = config.get("operations", None)
        if not transforms and not operations:
            return self.ok(req, processed=payload)
        try:
            if transforms:
                variables = dict(payload)
                for t in transforms:
                    ttype = t.get("type", "")
                    handler = _TRANSFORMS.get(ttype)
                    if handler is None: raise ValueError(f"Unknown transform: {ttype!r}")
                    src_data = None if ttype == "merge" else variables.get(t.get("source_var", ""))
                    if src_data is None and t.get("source_var", ""): raise ValueError(f"Variable '{t.get('source_var')}' not found")
                    rv = t.get("result_var", "")
                    if rv: variables[rv] = handler(src_data, t, variables)
                result = variables.get(config.get("output_var", ""), variables)
            else:
                data = copy.deepcopy(payload)
                ctx = {"_root": data}
                if "_input" in data: ctx["_input"] = data["_input"]
                for op_def in operations:
                    h = _LEGACY.get(op_def.get("op", ""))
                    if h is None: raise ValueError(f"Unknown operation: {op_def.get('op')!r}")
                    data = h(data, op_def, ctx)
                result = data
            if isinstance(result, dict):
                return self.ok(req, **result)
            return self.ok(req, processed=result)
        except Exception as e:
            log.exception("data_processing failed")
            if on_error == "continue":
                if isinstance(payload, dict):
                    return self.ok(req, **payload, error=str(e))
                return self.ok(req, processed=payload, error=str(e))
            return self.fail(req, error=f"{type(e).__name__}: {e}")