FROM python:3.11-slim

# Dépendances système (pdf2image/poppler)
RUN apt-get update && apt-get install -y --no-install-recommends \
    poppler-utils \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code et modules
COPY app.py cua_service.py intersections_parcelle.py report_from_json.py enclaves.py cerfa_vision_pipeline.py llm_utils.py fetch_plu_regulation.py ./

# Ressources (à la racine du repo)
COPY mapping_layers.json v_commune_2025.csv ./

# Dossier de sorties
RUN mkdir -p /app/output

# Variables d'env par défaut (tu peux les override au run)
ENV PORT=8080 \
    MAPPING_JSON_PATH=/app/mapping_layers.json \
    COMMUNES_CSV_PATH=/app/v_commune_2025.csv \
    ENABLE_CORS=1

EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
