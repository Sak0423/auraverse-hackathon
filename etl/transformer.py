# etl/transformer.py
from datetime import datetime

def _cast_value(val, target_type):
    if val is None:
        return None
    try:
        if target_type == "integer":
            return int(val)
        if target_type == "decimal":
            return float(val)
        if target_type == "boolean":
            if isinstance(val, bool):
                return val
            s = str(val).strip().lower()
            return s in ("1","true","yes","y")
        # default string
        return str(val)
    except Exception:
        return val  # fallback: return original

def transform_doc_best_effort(doc: dict, logical_doc: dict):
    """
    Attempt to produce a normalized dict according to logical_doc.fields_map.
    Unknown fields are kept under '_extra'.
    """
    if not isinstance(doc, dict):
        return {"_raw": doc}
    out = {}
    extras = {}
    fields_map = logical_doc.get("fields_map", {}) if isinstance(logical_doc, dict) else {}
    for k, v in doc.items():
        if k in fields_map:
            # choose type if available, else leave as-is
            types = fields_map[k].get("types", [])
            chosen = None
            if types:
                # prefer numeric if present
                if "int" in ",".join(types).lower() or "integer" in ",".join(types).lower():
                    chosen = "integer"
                elif "float" in ",".join(types).lower() or "decimal" in ",".join(types).lower():
                    chosen = "decimal"
                elif "bool" in ",".join(types).lower():
                    chosen = "boolean"
                else:
                    chosen = "string"
            if chosen:
                out[k] = _cast_value(v, chosen)
            else:
                out[k] = v
        else:
            extras[k] = v
    if extras:
        out["_extra"] = extras
    out["_normalized_at"] = datetime.utcnow().isoformat() + "Z"
    return out
