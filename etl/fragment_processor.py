# etl/fragment_processor.py
import os, json, uuid, datetime
from typing import List, Dict, Any
from etl.schema_generator import detect_storage_type, generate_sql_schema, generate_mongo_schema, generate_json_schema
from etl.sqlite_utils import apply_sql_ddl_and_insert


STORE_ROOT = os.path.join(os.getcwd(), "store")
PARSING_ROOT = os.path.join(STORE_ROOT, "parsing")

def _ensure_dir(path):
    os.makedirs(path, exist_ok=True)
    return path

def _nowz():
    return datetime.datetime.utcnow().isoformat() + "Z"

import datetime

def _rows_from_fragment(fragment_type: str, frag):
    """
    Normalize parser fragment into list of dict rows.
    fragment_type: one of "json", "csv", "html_table", "kv", "raw"
    frag: the fragment content from parser
    """
    rows = []
    try:
        if fragment_type == "json":
            # JSON fragments can be dict or list
            if isinstance(frag, dict):
                rows.append(frag)
            elif isinstance(frag, list):
                for v in frag:
                    if isinstance(v, dict):
                        rows.append(v)
            else:
                # fallback: wrap
                rows.append({"value": frag})
        elif fragment_type == "csv":
            # frag already a dict representing a single row (from our parser)
            if isinstance(frag, dict):
                rows.append(frag)
            elif isinstance(frag, list):
                for r in frag:
                    if isinstance(r, dict):
                        rows.append(r)
        elif fragment_type == "html_table":
            if isinstance(frag, dict):
                rows.append(frag)
            elif isinstance(frag, list):
                for r in frag:
                    if isinstance(r, dict):
                        rows.append(r)
        elif fragment_type == "kv":
            # frag is list of (k,v) tuples OR a dict
            if isinstance(frag, dict):
                rows.append(frag)
            elif isinstance(frag, (list, tuple)):
                try:
                    rows.append({k: v for k, v in frag})
                except Exception:
                    # try individual tuples
                    for pair in frag:
                        if isinstance(pair, (list, tuple)) and len(pair) >= 2:
                            rows.append({pair[0]: pair[1]})
            else:
                rows.append({"kv": str(frag)})
        else: # raw
            rows.append({"_text": str(frag)[:200]})
    except Exception:
        # defensive fallback
        rows.append({"_raw_fragment": str(frag)[:500]})
    return rows

def _write_fragment_output(source_id: str, file_id: str, frag_meta: dict):
    target = os.path.join(PARSING_ROOT, source_id, file_id)
    _ensure_dir(target)
    fname = f"fragment_{frag_meta['fragment_index']}_{frag_meta['id']}.json"
    path = os.path.join(target, fname)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(frag_meta, f, indent=2, ensure_ascii=False)
    return path

def process_fragments(source_id: str, file_id: str, fragments: dict, candidate_docs: List[Dict]=None, logical_doc: Dict=None):
    """
    Process each extracted fragment separately:
     - classify fragment as sql/nosql/object
     - convert into rows
     - generate target schema
     - write fragment metadata file under store/parsing/<source_id>/<file_id>/
    Returns list of fragment metadata dicts.
    """
    _ensure_dir(PARSING_ROOT)
    frag_list = []
    idx = 0
    # define fragment groups and types
    # json_fragments -> "json" (could be table-like or nested)
    # csv_fragments -> "csv"
    # html_tables -> "html_table"
    # kv_pairs -> "kv"
    # raw_text -> "raw" (one fragment)
    groups = [
        ("json", fragments.get("json_fragments", [])),
        ("csv", fragments.get("csv_fragments", [])),
        ("html_table", fragments.get("html_tables", [])),
        ("kv", fragments.get("kv_pairs", [])),
    ]
    # handle groups
    for typ, items in groups:
        if not items:
            continue
        for item in items:
            idx += 1
            rows = _rows_from_fragment(typ, item)
            # decide storage for this fragment
            frag_type, reason = detect_storage_type(rows, {"json_fragments": [], "html_tables": [], "csv_fragments": [], "kv_pairs": []})
            generated = {}
            if frag_type == "sql":
                gen = generate_sql_schema(table_name=f"{source_id}_t{idx}", docs=rows)
                generated["sql"] = gen
            elif frag_type == "nosql":
                gen = generate_mongo_schema(collection_name=f"{source_id}_c{idx}", docs=rows)
                generated["mongo"] = gen
            else:
                gen = generate_json_schema(rows)
                generated["json_schema"] = gen

            frag_meta = {
                "id": uuid.uuid4().hex[:8],
                "fragment_index": idx,
                "fragment_kind": typ,
                "rows_count": len(rows),
                "storage_decision": {"type": frag_type, "reason": reason},
                "generated": generated,
                "sample_rows": rows[:5],
                "created_at": datetime.datetime.utcnow().isoformat() + "Z"
            }

                        # ------------------ Auto-apply SQL fragment to SQLite (optional) ------------------
            try:
                # import here to avoid hard dependency if sqlite helper not present
                from etl.sqlite_utils import apply_sql_ddl_and_insert
                if frag_meta["storage_decision"]["type"] == "sql" and "sql" in generated:
                    ddl = generated["sql"].get("ddl", "")
                    if ddl:
                        # persistent DB per source (change path if you prefer in-memory)
                        db_dir = os.path.join(STORE_ROOT, "sqlite")
                        os.makedirs(db_dir, exist_ok=True)
                        db_path = os.path.join(db_dir, f"{source_id}.db")
                        # apply DDL and insert rows; returns dict with inserted count / error
                        applied = apply_sql_ddl_and_insert(ddl, rows, db_path=db_path, table_name=f"{source_id}_t{idx}")
                        frag_meta["applied_sql"] = applied
            except Exception as _e:
                # never fail fragment processing if DB apply breaks; record the error for debugging
                frag_meta.setdefault("applied_sql_error", str(_e))
            # -------------------------------------------------------------------------------
                        # ------------------ Auto-create Mongo collection & insert docs (optional) ------------------
            try:
                from etl.mongo_utils import create_collection_with_validator, insert_documents
                if frag_meta["storage_decision"]["type"] == "nosql" and "mongo" in generated:
                    validator = generated["mongo"].get("validator") or generated["mongo"].get("validator", None)
                    # default connection string and DB name (customize if needed)
                    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
                    MONGO_DB = os.getenv("MONGO_DB", "etl_auraverse")
                    col_name = f"{source_id}_c{idx}"
                    # create collection with validator (drop_if_exists optional)
                    create_res = create_collection_with_validator(MONGO_URI, MONGO_DB, col_name, validator, drop_if_exists=False)
                    frag_meta["mongo_create"] = create_res
                    # if creation ok, try inserting sample rows (safe: only up to first 200 rows)
                    if create_res.get("ok"):
                        try:
                            sample_to_insert = rows[:200]
                            insert_res = insert_documents(MONGO_URI, MONGO_DB, col_name, sample_to_insert)
                            frag_meta["mongo_insert"] = insert_res
                        except Exception as e:
                            frag_meta.setdefault("mongo_insert_error", str(e))
            except Exception as _e:
                frag_meta.setdefault("mongo_error", str(_e))
            # -----------------------------------------------------------------------------------------

            # persist fragment meta file
            _write_fragment_output(source_id, file_id, frag_meta)
            frag_list.append(frag_meta)

    # finally handle raw_text as a single fragment if present and not empty
    raw = fragments.get("raw_text")
    if raw:
        idx += 1
        rows = _rows_from_fragment("raw", raw)
        frag_type, reason = detect_storage_type(candidate_docs or [], {"json_fragments": [], "html_tables": [], "csv_fragments": [], "kv_pairs": []})
        # for raw we generally return object schema
        generated = {"json_schema": generate_json_schema(rows)}
        frag_meta = {
            "id": uuid.uuid4().hex[:8],
            "fragment_index": idx,
            "fragment_kind": "raw",
            "rows_count": len(rows),
            "storage_decision": {"type": frag_type, "reason": "raw_text_fallback"},
            "generated": generated,
            "sample_rows": rows[:1],
            "created_at": datetime.datetime.utcnow().isoformat() + "Z"
        }
        _write_fragment_output(source_id, file_id, frag_meta)
        frag_list.append(frag_meta)

    return frag_list
