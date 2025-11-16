# app/main.py

from fastapi import FastAPI
from app.api.upload import router as upload_router
from app.api.schema import router as schema_router
from app.api.records import router as records_router
from app.api.query import router as query_router
from app.api.errors import router as errors_router

app = FastAPI(title="ETL Auraverse - Enhanced")

app.include_router(upload_router, prefix="")
app.include_router(schema_router, prefix="")
app.include_router(records_router, prefix="")
app.include_router(query_router, prefix="")
app.include_router(errors_router, prefix="")

@app.get("/health")
async def health():
    return {"status": "ok", "time": __import__("datetime").datetime.utcnow().isoformat() + "Z"}
