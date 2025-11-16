# ETL Auraverse Hackathon — Minimal Starter

This repo is a minimal, runnable starter for the hackathon:
- FastAPI backend: `app/` with `/health` and `/ingest`
- Simple ETL stubs: `etl/` with parser, storage, transformer, schema_infer
- Streamlit frontend: `streamlit_app.py`
- Demo sample data: `demo/`
- Docker Compose for local dev

## Quick start (without Docker)
1. Create a virtualenv and install:
   ```
   python -m venv .venv
   .venv/scripts/activate
   pip install -r requirements.txt
   ```
2. Run the backend:
   ```
   uvicorn app.main:app --reload --port 8000
   ```
3. In another terminal run the Streamlit UI:
   ```
   streamlit run streamlit_app.py
   ```
4. Open Streamlit UI (it will print the URL) and upload/paste JSON to call `/ingest`.

## Quick start (with Docker Compose)
```
docker-compose up --build
```
- FastAPI on port 8000
- Streamlit on port 8501
- Mongo (optional) on port 27017

## What this starter does
- `POST /ingest` accepts JSON (or file upload via Streamlit) and writes a raw file to `raw/` with a generated `job_id`.
- Basic ETL stubs (parser, schema_infer, transformer) included for iterative improvement.

Happy hacking!
