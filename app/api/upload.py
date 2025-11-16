# app/api/upload.py
from etl.fragment_processor import process_fragments
from etl.schema_generator import detect_storage_type, generate_sql_schema, generate_mongo_schema, generate_json_schema
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from typing import Optional
import uuid, os, json, datetime, traceback
from etl.parser import extract_fragments_from_bytes
from etl.schema_registry import SchemaRegistry
from etl.storage import write_raw, write_records

router = APIRouter()
SR = SchemaRegistry(storage_root=os.path.join(os.getcwd(), "store"))

ERROR_LOG = os.path.join(os.getcwd(), "store", "errors.log")
os.makedirs(os.path.dirname(ERROR_LOG), exist_ok=True)

def _log_exception(exc: Exception, context: dict = None) -> str:
    """Write full traceback + context to store/errors.log and return unique id."""
    err_id = f"err_{uuid.uuid4().hex[:8]}"
    now = datetime.datetime.utcnow().isoformat() + "Z"
    tb = traceback.format_exc()
    entry = {
        "id": err_id,
        "time": now,
        "context": context or {},
        "traceback": tb,
        "exc_str": str(exc),
    }
    try:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        # last resort: print to stdout if file cannot be written
        print("Failed to write error log:", entry)
    return err_id

@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    source_id: Optional[str] = Form(None),
    version: Optional[str] = Form(None),
):
    context = {"filename": getattr(file, "filename", None)}
    try:
        # Basic validations
        if not source_id:
            source_id = f"src_{uuid.uuid4().hex[:8]}"
        file_bytes = await file.read()
        mimetype = file.content_type or "application/octet-stream"
        file_id = f"{uuid.uuid4().hex}"
        received_at = datetime.datetime.utcnow().isoformat() + "Z"

        # Save raw file metadata + contents
        raw_meta = {
            "file_id": file_id,
            "source_id": source_id,
            "filename": file.filename,
            "mimetype": mimetype,
            "received_at": received_at,
        }
        raw_path = write_raw(file_id, raw_meta, file_bytes)

        # Extract structured fragments
        fragments = extract_fragments_from_bytes(file_bytes, filename=file.filename, mimetype=mimetype)
        if not isinstance(fragments, dict):
            raise RuntimeError("parser.extract_fragments_from_bytes returned non-dict")

        # Build candidate docs to infer schema (use JSON fragments + kv_pairs as docs)
        candidate_docs = []
        for j in fragments.get("json_fragments", []):
            if isinstance(j, dict):
                candidate_docs.append(j)
        # convert kv pairs to dict docs
        if fragments.get("kv_pairs"):
            try:
                candidate_docs.append({k: v for k, v in fragments["kv_pairs"]})
            except Exception:
                pass

        # fallback: use raw_text as single doc
        if not candidate_docs and fragments.get("raw_text"):
            candidate_docs.append({"_text": fragments["raw_text"][:500]})

        # --- SCHEMA EVOLUTION (use evolve_schema to produce logical + instance schemas)
        logical_id, instance_id, logical_doc, instance_doc, diff = SR.evolve_schema(source_id, candidate_docs, file_id=file_id)

        # Persist extracted records with schema tags and pass logical_doc for normalization
        saved_count = write_records(source_id, file_id, logical_id, instance_id, fragments, logical_doc=logical_doc)

        # process fragments individually and write per-fragment schema+samples
        fragment_results = process_fragments(source_id, file_id, fragments, candidate_docs=candidate_docs, logical_doc=logical_doc)

        # Decide storage type & generate target schema
        storage_decision, decision_reason = detect_storage_type(candidate_docs, fragments)
        generated = {}
        if storage_decision == "sql":
            # if CSV/HTML tables are present, prefer those rows for column detection
            table_rows = []
            if fragments.get("csv_fragments"):
                table_rows = fragments.get("csv_fragments")
            elif fragments.get("html_tables"):
                table_rows = fragments.get("html_tables")
            elif candidate_docs:
                table_rows = candidate_docs
            gen = generate_sql_schema(table_name=f"{source_id}_table", docs=table_rows)
            generated["sql"] = gen

        elif storage_decision == "nosql":
            gen = generate_mongo_schema(collection_name=f"{source_id}_col", docs=candidate_docs)
            generated["mongo"] = gen

        else:
            gen = generate_json_schema(candidate_docs)
            generated["json_schema"] = gen

        parsed_summary = {
            "json_fragments": len(fragments.get("json_fragments", [])),
            "html_tables": len(fragments.get("html_tables", [])),
            "csv_fragments": len(fragments.get("csv_fragments", [])),
            "kv_pairs": len(fragments.get("kv_pairs", [])),
            "records_saved": saved_count,
        }

        response = {
            "status": "ok",
            "source_id": source_id,
            "file_id": file_id,
            "schema_logical_id": logical_id,
            "schema_instance_id": instance_id,
            "parsed_fragments_summary": parsed_summary,
            "schema_diff": diff,
            "storage_decision": {"type": storage_decision, "reason": decision_reason},
            "generated_schema": generated,
            "fragment_parsing": fragment_results
        }
        return response

    except Exception as exc:
        # Log full traceback and return helpful message
        context.update({"source_id": source_id if 'source_id' in locals() else None})
        err_id = _log_exception(exc, context=context)
        # Return a short, safe error message to the client
        raise HTTPException(status_code=500, detail={
            "error_short": "internal_server_error",
            "error_id": err_id,
            "hint": "check store/errors.log for full traceback"
        })
