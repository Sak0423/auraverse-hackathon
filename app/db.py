# Minimal DB layer placeholder - currently filesystem-based
import os, json
RAW_DIR = os.path.join(os.getcwd(), "raw")
NORMALIZED_DIR = os.path.join(os.getcwd(), "normalized")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(NORMALIZED_DIR, exist_ok=True)

def save_raw(job_id: str, record: dict):
    path = os.path.join(RAW_DIR, f"{job_id}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f)
    return path
