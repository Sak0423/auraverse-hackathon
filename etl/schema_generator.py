# etl/schema_generator.py
import re, json
from dateutil import parser as dateparser
from dataclasses import dataclass
from typing import List, Dict, Any

# --- TUNABLES ---------------------------------------------------------------
@dataclass
class StorageDecisionConfig:
    # fraction of docs that must share the same keyset to be considered "consistent tabular"
    CONSISTENCY_THRESHOLD: float = 0.7
    # minimum columns required to consider SQL/table
    MIN_COLUMNS_FOR_SQL: int = 1
    # minimum number of sample docs to decide SQL by consistency
    MIN_DOCS_FOR_CONSISTENCY: int = 2
    # fraction of date-like occurrences to mark field as date
    DATE_THRESHOLD: float = 0.6
    # fraction of ints to mark integer type
    INT_THRESHOLD: float = 0.8
    # fraction of floats to mark decimal
    FLOAT_THRESHOLD: float = 0.5
    # treat small arrays/objects as scalar? set False to be strict
    TREAT_LIST_AS_NONSCALAR: bool = True

# create default config instance (import and modify this)
CFG = StorageDecisionConfig()
# ----------------------------------------------------------------------------

# --- type detection helpers -------------------------------------------------
def _is_int(v) -> bool:
    try:
        if isinstance(v, bool): return False
        int(v)
        return True
    except: return False

def _is_float(v) -> bool:
    try:
        float(v)
        return not _is_int(v)
    except: return False

def _is_bool(v) -> bool:
    if isinstance(v, bool): return True
    s = str(v).strip().lower()
    return s in ("true","false","1","0","yes","no","y","n")

def _is_date(v) -> bool:
    if v is None: return False
    if isinstance(v, (int, float)): return False
    s = str(v).strip()
    if len(s) < 4 or len(s) > 40:
        return False
    try:
        dateparser.parse(s, fuzzy=False)
        return True
    except Exception:
        return False

def _is_scalar(v) -> bool:
    if isinstance(v, (dict, list)) and not CFG.TREAT_LIST_AS_NONSCALAR:
        return True
    return not isinstance(v, (list, dict))

# --- storage decision ------------------------------------------------------
def detect_storage_type(sample_docs: List[Dict[str, Any]], fragments: Dict[str, Any]):
    """
    Decide: 'sql', 'nosql', or 'object' using CFG thresholds.
    Returns (decision, reason).
    """
    # explicit table fragments -> SQL
    if fragments.get("csv_fragments") and len(fragments.get("csv_fragments")) > 0:
        return "sql", "contains csv fragments"
    if fragments.get("html_tables") and len(fragments.get("html_tables")) > 0:
        return "sql", "contains html tables"

    docs = [d for d in (sample_docs or []) if isinstance(d, dict)]
    if not docs:
        if fragments.get("kv_pairs"):
            return "object", "only key:value pairs"
        return "object", "no structured docs found"

    # detect nested structures
    nested_count = 0
    keysets = [set(d.keys()) for d in docs]
    for d in docs:
        for v in d.values():
            if isinstance(v, (dict, list)):
                nested_count += 1
                break

    # keyset consistency
    base = keysets[0] if keysets else set()
    same_keys = sum(1 for s in keysets if s == base)
    consistency = same_keys / len(keysets) if keysets else 0.0

    # heuristics with CFG
    if nested_count > 0:
        return "nosql", f"found nested structures in {nested_count} docs"
    if len(docs) >= CFG.MIN_DOCS_FOR_CONSISTENCY and consistency >= CFG.CONSISTENCY_THRESHOLD and len(base) >= CFG.MIN_COLUMNS_FOR_SQL:
        all_scalar = all(all(_is_scalar(v) for v in d.values()) for d in docs)
        if all_scalar:
            return "sql", f"consistent flat rows ({len(base)} cols, consistency={consistency:.2f})"
        else:
            return "nosql", "flat keys but some non-scalar values"
    return "object", "no consistent tabular structure detected"

# --- infer field types -----------------------------------------------------
def infer_field_types(docs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Return dict: field -> {type, example, count}
    types: integer, decimal, boolean, date, string, object, array
    """
    stats = {}
    for d in docs:
        if not isinstance(d, dict):
            continue
        for k, v in d.items():
            if k not in stats:
                stats[k] = {"counts": 0, "int": 0, "float": 0, "bool": 0, "date": 0, "object":0, "array":0, "sample": v}
            s = stats[k]
            s["counts"] += 1
            if _is_int(v):
                s["int"] += 1
            elif _is_float(v):
                s["float"] += 1
            elif _is_bool(v):
                s["bool"] += 1
            elif _is_date(v):
                s["date"] += 1
            elif isinstance(v, dict):
                s["object"] += 1
            elif isinstance(v, list):
                s["array"] += 1
            else:
                pass
    out = {}
    for k, s in stats.items():
        c = s["counts"]
        if s["object"] > 0:
            t = "object"
        elif s["array"] > 0:
            t = "array"
        elif (s["date"] / c) >= CFG.DATE_THRESHOLD:
            t = "date"
        elif (s["int"] / c) >= CFG.INT_THRESHOLD:
            t = "integer"
        elif (s["float"] / c) >= CFG.FLOAT_THRESHOLD or ((s["int"] + s["float"]) / c >= 0.8 and s["float"] > 0):
            t = "decimal"
        elif (s["bool"] / c) >= 0.8:
            t = "boolean"
        else:
            t = "string"
        out[k] = {"type": t, "example": s["sample"], "count": c}
    return out

# --- SQL type mapping ------------------------------------------------------
def _sql_type_from_inferred(t: str) -> str:
    if t == "integer":
        return "INTEGER"
    if t == "decimal":
        return "DECIMAL"
    if t == "boolean":
        return "BOOLEAN"
    if t == "date":
        return "TIMESTAMP"
    return "TEXT"

# --- generators ------------------------------------------------------------
def generate_sql_schema(table_name: str, docs: List[Dict[str, Any]]):
    """
    Generate a CREATE TABLE statement from docs (list of dicts).
    Returns {"ddl": "...", "fields": {...}}
    """
    if not docs:
        return {"ddl": "", "reason": "no docs", "fields": {}}
    field_types = infer_field_types(docs)
    cols = []
    for k, meta in field_types.items():
        colname = re.sub(r'[^A-Za-z0-9_]', '_', k).lower()
        sqltype = _sql_type_from_inferred(meta["type"])
        cols.append(f'    "{colname}" {sqltype}')
    ddl = f'CREATE TABLE "{table_name}" (\n' + ",\n".join(cols) + "\n);"
    return {"ddl": ddl, "fields": field_types}

def generate_mongo_schema(collection_name: str, docs: List[Dict[str, Any]]):
    """
    Generate a Mongo JSON Schema validator (suitable for createCollection validator).
    Returns {"validator": {...}, "fields": {...}}
    """
    field_types = infer_field_types(docs)
    properties = {}
    for k, meta in field_types.items():
        t = meta["type"]
        if t == "integer":
            prop = {"bsonType": "int", "description": f"type: {t}"}
        elif t == "decimal":
            prop = {"bsonType": ["double","decimal"], "description": f"type: {t}"}
        elif t == "boolean":
            prop = {"bsonType": "bool"}
        elif t == "date":
            prop = {"bsonType": "date"}
        elif t == "object":
            prop = {"bsonType": "object"}
        elif t == "array":
            prop = {"bsonType": "array"}
        else:
            prop = {"bsonType": "string"}
        properties[k] = prop
    schema = {
        "$jsonSchema": {
            "bsonType": "object",
            "properties": properties,
        }
    }
    return {"validator": schema, "fields": field_types}

def generate_json_schema(docs: List[Dict[str, Any]]):
    """
    Produce a JSON Schema (simple) from docs.
    Returns {"json_schema": {...}, "fields": {...}}
    """
    field_types = infer_field_types(docs)
    props = {}
    for k, meta in field_types.items():
        t = meta["type"]
        if t == "integer":
            jt = "integer"
        elif t == "decimal":
            jt = "number"
        elif t == "boolean":
            jt = "boolean"
        elif t == "date":
            jt = "string"
            props[k] = {"type": jt, "format": "date-time"}
            continue
        elif t == "object":
            jt = "object"
        elif t == "array":
            jt = "array"
        else:
            jt = "string"
        props[k] = {"type": jt}
    schema = {"type": "object", "properties": props}
    return {"json_schema": schema, "fields": field_types}
