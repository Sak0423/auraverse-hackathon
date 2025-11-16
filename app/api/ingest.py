from fastapi import APIRouter, Request, UploadFile, File, Form
from pydantic import BaseModel
import uuid, os, json, datetime

router = APIRouter()

RAW_DIR = os.path.join(os.getcwd(), "raw")
os.makedirs(RAW_DIR, exist_ok=True)

class IngestResponse(BaseModel):
    job_id: str
    received_at: str
    raw_path: str

@router.post("/ingest", response_model=IngestResponse)
async def ingest(request: Request):
    # Accept JSON body or form file upload
    content_type = request.headers.get("content-type", "")
    data = None
    if "multipart/form-data" in content_type:
        form = await request.form()
        if "file" in form:
            uploaded = form["file"]
            body = await uploaded.read()
            try:
                data = json.loads(body.decode("utf-8"))
            except Exception:
                data = {"_raw": body.decode("utf-8")}
        elif "json" in form:
            try:
                data = json.loads(form["json"])
            except Exception:
                data = {"_raw": form["json"]}
    else:
        try:
            data = await request.json()
        except Exception:
            text = await request.body()
            data = {"_raw": text.decode("utf-8")}

    job_id = str(uuid.uuid4())
    received_at = datetime.datetime.utcnow().isoformat() + "Z"
    raw_path = os.path.join(RAW_DIR, f"{job_id}.json")
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump({"job_id": job_id, "received_at": received_at, "payload": data}, f, ensure_ascii=False, indent=2)

    return IngestResponse(job_id=job_id, received_at=received_at, raw_path=raw_path)
