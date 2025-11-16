# app/api/query.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from etl.storage import load_records_for_source
from typing import Any, Dict
import re

router = APIRouter()

class NLQuery(BaseModel):
    source_id: str
    nl_query: str
    schema_id: str = None
    limit: int = 50

def simple_translate(nl: str):
    # Very small deterministic translator for demo/test.
    # Examples supported:
    # - "find where name is Alice"
    # - "find products with price > 10"
    m = re.search(r"where\s+(\w+)\s+is\s+([\\w@.\\-]+)", nl, re.I)
    if m:
        return {"op": "eq", "field": m.group(1), "value": m.group(2)}
    m2 = re.search(r"(\w+)\s*>\s*([0-9\.]+)", nl)
    if m2:
        return {"op": "gt", "field": m2.group(1), "value": float(m2.group(2))}
    # fallback: search keyword presence
    words = nl.strip().split()
    return {"op": "contains", "term": words[-1] if words else nl}

@router.post("/query")
def query_endpoint(q: NLQuery):
    recs = load_records_for_source(q.source_id, q.schema_id)
    if recs is None:
        raise HTTPException(status_code=404, detail="no records")
    rule = simple_translate(q.nl_query)
    results = []
    if rule["op"] == "eq":
        for r in recs:
            # naive deep search
            if str(r.get(rule["field"], "")).lower() == str(rule["value"]).lower():
                results.append(r)
    elif rule["op"] == "gt":
        for r in recs:
            try:
                if float(r.get(rule["field"], 0)) > rule["value"]:
                    results.append(r)
            except:
                pass
    else:
        term = rule.get("term", "").lower()
        for r in recs:
            if any(term in str(v).lower() for v in r.values()):
                results.append(r)
    return {"source_id": q.source_id, "nl_query": q.nl_query, "hits": len(results), "results": results[: q.limit]}
