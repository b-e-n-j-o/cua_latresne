FROM python:3.11-slim

# Dépendances système (géométries + pdf)
RUN apt-get update && apt-get install -y --no-install-recommends \
    poppler-utils \
    g++ \
    libgeos-dev \
    proj-bin \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dépendances Python
COPY requirements.txt .
# ⚠️ pip install supplémentaire uniquement si pas déjà listés
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir \
       python-docx \
       supabase \
       python-dotenv \
       shapely \
       pyproj

# -------------------------------
# 📦 Copie du code et des ressources
# -------------------------------
COPY app.py /app/
COPY cua_orchestrator.py /app/
COPY path_setup.py /app/
COPY CONFIG/ /app/CONFIG/
COPY CUA_GENERATION/ /app/CUA_GENERATION/
COPY INTERSECTIONS/ /app/INTERSECTIONS/
COPY MAP_GENERATION/ /app/MAP_GENERATION/
COPY PIPELINE_VISION/ /app/PIPELINE_VISION/
COPY UTILS/ /app/UTILS/
COPY templates/ /app/templates/

# ⚠️ supprime les .pyc/__pycache__ pour éviter d’embarquer du bruit
RUN find /app -type d -name "__pycache__" -exec rm -rf {} +

# Dossier de sorties
RUN mkdir -p /app/output

# Variables d'env par défaut (override au run)
ENV PORT=8080 \
    MAPPING_JSON_PATH=/app/CONFIG/mapping_layers.json \
    COMMUNES_CSV_PATH=/app/CONFIG/v_commune_2025.csv \
    ENABLE_CORS=1 \
    PYTHONPATH=/app

EXPOSE 8080

# Point d'entrée : app.py avec uvicorn (FastAPI ou Starlette présumé)
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
