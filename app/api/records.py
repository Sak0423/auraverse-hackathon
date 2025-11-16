# app/api/records.py
from fastapi import APIRouter, Query, HTTPException
from typing import Optional
import os, json
from etl.storage import load_records_for_source

router = APIRouter()

@router.get("/records")
def get_records(source_id: str = Query(...), schema_id: Optional[str] = Query(None), limit: int = 50, offset: int = 0):
    recs = load_records_for_source(source_id, schema_id)
    if recs is None:
        raise HTTPException(status_code=404, detail="No records found")
    # simple pagination
    slice_ = recs[offset: offset + limit]
    return {"source_id": source_id, "schema_id": schema_id, "count": len(recs), "records": slice_}
