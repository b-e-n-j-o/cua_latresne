# app.py
# -*- coding: utf-8 -*-
import os, time, uuid, logging
from pathlib import Path
from typing import Optional, List

from fastapi import FastAPI, File, UploadFile, Form, HTTPException, Depends, Header, Body, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from cua_service import (
    OUTPUT_DIR,
    health_check,
    run_cua_from_pdf,
    run_cua_direct,
)

APP_NAME = "Kerelia CUA API"
app = FastAPI(title=APP_NAME)

# Logger simple
log = logging.getLogger("cua.api")
if not log.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

# Middleware de logs HTTP
@app.middleware("http")
async def log_requests(request: Request, call_next):
    req_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    start = time.perf_counter()
    body_preview = ""
    try:
        ct = request.headers.get("content-type", "")
        if "application/json" in ct:
            raw = await request.body()
            body_preview = raw.decode("utf-8", errors="replace")
            if len(body_preview) > 1000:
                body_preview = body_preview[:1000] + "…"
    except Exception:
        pass
    log.info(f"➡️ {request.method} {request.url.path} id={req_id} "
             f"len={request.headers.get('content-length','?')} ct={request.headers.get('content-type','-')}"
             + (f" body={body_preview}" if body_preview else ""))
    try:
        response = await call_next(request)
    except Exception as e:
        dur = (time.perf_counter() - start) * 1000
        log.exception(f"❌ {request.method} {request.url.path} id={req_id} failed in {dur:.1f} ms: {e}")
        raise
    dur = (time.perf_counter() - start) * 1000
    log.info(f"✅ {request.method} {request.url.path} id={req_id} -> {response.status_code} in {dur:.1f} ms")
    response.headers["X-Request-ID"] = req_id
    return response

# CORS (optionnel)
if os.getenv("ENABLE_CORS", "0") == "1":
    origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",")]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# Fichiers générés
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
app.mount("/files", StaticFiles(directory=str(OUTPUT_DIR), html=True), name="files")

# Sécurité simple
def api_key_guard(x_api_key: Optional[str] = Header(None)):
    expected = os.getenv("API_AUTH_TOKEN", "")
    if expected and x_api_key != expected:
        raise HTTPException(401, "Invalid or missing API key")
    return True

# Pydantic pour /cua/direct
class DirectCUAPayload(BaseModel):
    parcel: str = Field(..., description="Parcelle(s) 'SECTION NUM', ex: 'AC 0496' ou 'AC 0496, AC 0497'")
    insee: str = "33234"
    commune: str = "Latresne"
    mapping_path: Optional[str] = None
    schema_whitelist: List[str] = ["public"]
    values_limit: int = 100
    carve_enclaves: bool = True
    enclave_buffer_m: float = 120.0
    make_report: bool = True
    make_map: bool = True
    max_features_per_layer_on_map: int = 200

# ========================= Endpoints ========================= #
@app.get("/health")
def health():
    return health_check()

@app.post("/cua")
def cua_endpoint(
    file: UploadFile = File(..., description="CERFA PDF"),
    schema_whitelist: Optional[str] = Form("public"),
    values_limit: int = Form(100),
    carve_enclaves: int = Form(1),
    enclave_buffer_m: float = Form(120.0),
    make_report: int = Form(1),
    make_map: int = Form(1),
    max_features_per_layer_on_map: int = Form(200),
    _auth = Depends(api_key_guard)
):
    if file.content_type not in ("application/pdf", "application/octet-stream"):
        raise HTTPException(400, "Fichier PDF invalide")
    try:
        payload = run_cua_from_pdf(
            pdf_bytes=file.file.read(),
            filename=file.filename or "cerfa.pdf",
            schema_whitelist=schema_whitelist,
            values_limit=values_limit,
            carve_enclaves=bool(int(carve_enclaves)),
            enclave_buffer_m=float(enclave_buffer_m),
            make_report=bool(int(make_report)),
            make_map=bool(int(make_map)),
            max_features_per_layer_on_map=int(max_features_per_layer_on_map),
        )
        return JSONResponse(payload)
    except Exception as e:
        raise HTTPException(500, f"Erreur /cua: {e}")

@app.post("/cua/direct")
def cua_direct(payload: DirectCUAPayload = Body(...)):
    try:
        out = run_cua_direct(
            parcel=payload.parcel,
            insee=payload.insee,
            commune=payload.commune,
            mapping_path=payload.mapping_path,
            schema_whitelist=payload.schema_whitelist,
            values_limit=payload.values_limit,
            carve_enclaves=payload.carve_enclaves,
            enclave_buffer_m=payload.enclave_buffer_m,
            make_report=payload.make_report,
            make_map=payload.make_map,
        )
        return JSONResponse(out)
    except Exception as e:
        raise HTTPException(500, f"Erreur /cua/direct: {e}")
