# etl/storage.py
import os, json
from datetime import datetime
from etl.transformer import transform_doc_best_effort

ROOT = os.path.join(os.getcwd(), "store")
RAW_DIR = os.path.join(ROOT, "raw")
RECORDS_DIR = os.path.join(ROOT, "records")
NORMALIZED_DIR = os.path.join(ROOT, "normalized")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(RECORDS_DIR, exist_ok=True)
os.makedirs(NORMALIZED_DIR, exist_ok=True)

def write_raw(file_id: str, meta: dict, content_bytes: bytes):
    path = os.path.join(RAW_DIR, f"{file_id}.bin")
    with open(path, "wb") as f:
        f.write(content_bytes)
    # also save meta
    try:
        with open(os.path.join(RAW_DIR, f"{file_id}.meta.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)
    except Exception:
        pass
    return path

def write_records(source_id: str, file_id: str, logical_id: str, instance_schema_id: str, fragments: dict, logical_doc: dict = None):
    """
    Persist extracted fragment records:
     - per-file: store/records/<source_id>/<file_id>.records.json
     - aggregated raw index: store/records/<source_id>/index.json
     - normalized index (best-effort): store/normalized/<source_id>/index.json (if logical_doc provided)
    Returns number of raw fragment records saved.
    """
    target = os.path.join(RECORDS_DIR, source_id)
    os.makedirs(target, exist_ok=True)
    out = []

    def _prepare(rec):
        rec = dict(rec) if isinstance(rec, dict) else {"_value": rec}
        rec["_file_id"] = file_id
        rec["_logical_id"] = logical_id
        rec["_instance_schema_id"] = instance_schema_id
        return rec

    # collect fragments
    for j in fragments.get("json_fragments", []):
        out.append(_prepare(j))
    for c in fragments.get("csv_fragments", []):
        out.append(_prepare(c))
    for t in fragments.get("html_tables", []):
        out.append(_prepare(t))
    if fragments.get("kv_pairs"):
        try:
            kv = {k: v for k, v in fragments["kv_pairs"]}
            out.append(_prepare(kv))
        except Exception:
            pass
    if fragments.get("raw_text"):
        out.append(_prepare({"_text": fragments.get("raw_text")[:200]}))

    # write per-file record file
    try:
        path = os.path.join(target, f"{file_id}.records.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
    except Exception:
        pass

    # append to aggregated raw index
    index_path = os.path.join(target, "index.json")
    existing = []
    if os.path.exists(index_path):
        try:
            with open(index_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = []
    existing.extend(out)
    try:
        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)
    except Exception:
        pass

    # produce best-effort normalized records if logical_doc provided
    if logical_doc:
        n_target = os.path.join(NORMALIZED_DIR, source_id)
        os.makedirs(n_target, exist_ok=True)
        n_index_path = os.path.join(n_target, "index.json")
        n_existing = []
        if os.path.exists(n_index_path):
            try:
                with open(n_index_path, "r", encoding="utf-8") as f:
                    n_existing = json.load(f)
            except Exception:
                n_existing = []
        normalized_out = []
        for rec in out:
            try:
                # transform_doc_best_effort expects original doc without meta,
                # so remove our internal keys when normalizing.
                raw_doc = {k: v for k, v in rec.items() if not k.startswith("_")}
                norm = transform_doc_best_effort(raw_doc, logical_doc)
                # include meta
                norm["_file_id"] = rec.get("_file_id")
                norm["_logical_id"] = rec.get("_logical_id")
                norm["_instance_schema_id"] = rec.get("_instance_schema_id")
                normalized_out.append(norm)
            except Exception:
                normalized_out.append({"_raw": rec})
        n_existing.extend(normalized_out)
        try:
            with open(n_index_path, "w", encoding="utf-8") as f:
                json.dump(n_existing, f, indent=2)
        except Exception:
            pass

    return len(out)

def load_records_for_source(source_id: str, schema_id: str = None):
    """
    Return the aggregated raw records for a source (list).
    If schema_id provided, filter by _instance_schema_id or _logical_id.
    """
    idx = os.path.join(RECORDS_DIR, source_id, "index.json")
    if not os.path.exists(idx):
        return None
    try:
        with open(idx, "r", encoding="utf-8") as f:
            arr = json.load(f)
    except Exception:
        return None
    if schema_id:
        filtered = [r for r in arr if r.get("_instance_schema_id") == schema_id or r.get("_logical_id") == schema_id]
        return filtered
    return arr

def load_normalized_for_source(source_id: str, schema_id: str = None):
    """
    Return normalized (best-effort) records for a source, if available.
    """
    idx = os.path.join(NORMALIZED_DIR, source_id, "index.json")
    if not os.path.exists(idx):
        return None
    try:
        with open(idx, "r", encoding="utf-8") as f:
            arr = json.load(f)
    except Exception:
        return None
    if schema_id:
        filtered = [r for r in arr if r.get("_instance_schema_id") == schema_id or r.get("_logical_id") == schema_id]
        return filtered
    return arr
