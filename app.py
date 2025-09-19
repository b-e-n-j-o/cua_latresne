# app.py
import os, time, uuid, logging
from pathlib import Path
from typing import Optional, List

from fastapi import FastAPI, File, UploadFile, Form, HTTPException, Depends, Header, Body, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# Importez uniquement l'orchestrateur qui contient tout le reste
from cua_orchestrator import run_full_pipeline, run_parcel_pipeline

# Vous pouvez également importer l'OUTPUT_DIR si vous le définissez dans l'orchestrator pour plus de centralisation
# from cua_orchestrator import OUTPUT_DIR 

APP_NAME = "Kerelia CUA API"
app = FastAPI(title=APP_NAME)

# --- Middleware, CORS, Sécurité (Logique qui reste ici) ---
log = logging.getLogger("cua.api")
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

@app.middleware("http")
async def log_requests(request: Request, call_next):
    req_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    start = time.perf_counter()
    response = await call_next(request)
    dur = (time.perf_counter() - start) * 1000
    log.info(f"✅ {request.method} {request.url.path} id={req_id} -> {response.status_code} in {dur:.1f} ms")
    response.headers["X-Request-ID"] = req_id
    return response

if os.getenv("ENABLE_CORS", "0") == "1":
    origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",")]
    app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

OUTPUT_DIR = Path.cwd() / "output" # Géré ici ou dans l'orchestrator
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
app.mount("/files", StaticFiles(directory=str(OUTPUT_DIR), html=True), name="files")

def api_key_guard(x_api_key: Optional[str] = Header(None)):
    expected = os.getenv("API_AUTH_TOKEN", "")
    if expected and x_api_key != expected:
        raise HTTPException(401, "Invalid or missing API key")
    return True

# --- Modèles Pydantic (Logique qui reste ici) ---
class DirectCUAPayload(BaseModel):
    parcel: str = Field(..., description="Parcelle(s) 'SECTION NUM'")
    insee: str
    commune: str
    user_id: Optional[str] = None
    user_email: Optional[str] = None
    notify_emails: Optional[str] = None

# --- Endpoints ---
@app.get("/health")
def health():
    # Cet endpoint peut rester ici pour une vérification rapide de l'API
    return {"status": "ok", "app": APP_NAME}

@app.post("/cua")
def process_cua(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="CERFA PDF"),
    user_id: Optional[str] = Form(None),
    user_email: Optional[str] = Form(None),
    notify_emails: Optional[str] = Form(None),
    _auth = Depends(api_key_guard)
):
    try:
        # Créer un répertoire temporaire unique pour ce job
        temp_dir = Path("temp") / str(uuid.uuid4())
        temp_dir.mkdir(parents=True, exist_ok=True)
        pdf_path = temp_dir / file.filename
        
        # Sauvegarder le fichier PDF
        with pdf_path.open("wb") as buffer:
            buffer.write(file.file.read())
            
        # Déclencher l'orchestrateur en tâche de fond
        background_tasks.add_task(
            run_full_pipeline,
            pdf_path=str(pdf_path),
            temp_dir=temp_dir,
            insee_csv_path="CONFIG/v_commune_2025.csv",
            mapping_path="CONFIG/mapping_layers.json",
            user_id=user_id,
            notify_emails=[e.strip() for e in ((user_email or "") + "," + (notify_emails or "")).split(",") if e.strip()]
        )
        
        return JSONResponse({"status": "processing", "message": "Votre demande est en cours de traitement."})
    except Exception as e:
        log.error(f"Erreur dans l'endpoint /cua: {e}")
        raise HTTPException(500, f"Erreur lors du traitement : {e}")

@app.post("/cua/direct")
def process_cua_direct(
    background_tasks: BackgroundTasks,
    payload: DirectCUAPayload = Body(...),
    _auth = Depends(api_key_guard)
):
    try:
        # Créer un répertoire temporaire unique pour ce job
        temp_dir = Path("temp") / str(uuid.uuid4())
        
        # Déclencher l'orchestrateur en tâche de fond
        background_tasks.add_task(
            run_parcel_pipeline,
            parcels=payload.parcel,
            insee=payload.insee,
            commune=payload.commune,
            temp_dir=temp_dir,
            insee_csv_path="CONFIG/v_commune_2025.csv",
            mapping_path="CONFIG/mapping_layers.json",
            user_id=payload.user_id,
            notify_emails=[e.strip() for e in ((payload.user_email or "") + "," + (payload.notify_emails or "")).split(",") if e.strip()]
        )
        
        return JSONResponse({"status": "processing", "message": "Votre demande directe est en cours de traitement."})
    except Exception as e:
        log.error(f"Erreur dans l'endpoint /cua/direct: {e}")
        raise HTTPException(500, f"Erreur lors du traitement : {e}")

from sqlalchemy import text as sql_text

@app.get("/jobs/{job_id}")
def get_job(job_id: int, _auth = Depends(api_key_guard)):
    engine = _get_db_engine()
    if not engine:
        raise HTTPException(500, "DB not available")
    with engine.begin() as con:
        row = con.execute(sql_text("""
            SELECT id, status, report_docx_path, map_html_path
            FROM public.cua_jobs
            WHERE id = :id
        """), {"id": job_id}).mappings().first()
        if not row:
            raise HTTPException(404, "Job not found")
        return dict(row)
