from pydantic import BaseModel

class RawRecord(BaseModel):
    job_id: str
    received_at: str
    payload: dict
