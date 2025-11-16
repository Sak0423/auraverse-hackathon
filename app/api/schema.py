# app/api/schema.py
from fastapi import APIRouter, Query, HTTPException
import os
from etl.schema_registry import SchemaRegistry

router = APIRouter()
SR = SchemaRegistry(storage_root=os.path.join(os.getcwd(), "store"))

@router.get("/schema")
def get_schema(source_id: str = Query(...)):
    schema = SR.get_logical_schema(source_id)
    if not schema:
        raise HTTPException(status_code=404, detail="No logical schema for source_id")
    return schema

@router.get("/schema/history")
def get_schema_history(source_id: str = Query(...)):
    history = SR.get_schema_history(source_id)
    return {"source_id": source_id, "history": history}
