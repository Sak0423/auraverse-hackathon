# app/api/errors.py
from fastapi import APIRouter, HTTPException
import os, json

router = APIRouter()

ERROR_LOG = os.path.join(os.getcwd(), "store", "errors.log")

@router.get("/last_error")
def last_error():
    if not os.path.exists(ERROR_LOG):
        raise HTTPException(status_code=404, detail="no error log")
    lines = []
    # read lines, return last JSON-like entry
    with open(ERROR_LOG, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                j = json.loads(line)
                lines.append(j)
            except Exception:
                # skip non-json lines
                continue
    if not lines:
        raise HTTPException(status_code=404, detail="no json errors found")
    return lines[-1]
