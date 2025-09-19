#!/usr/bin/env bash
set -e

IMAGE_NAME="myapp:latest"
CONTAINER_NAME="myapp-local"

echo "🧹 Nettoyage fichiers macOS..."
find . -name "._*" -delete
find . -name ".DS_Store" -delete
find . -name ".AppleDouble" -delete

echo "🛠️  Build de l'image..."
docker build -t $IMAGE_NAME .

echo "🚀 Lancement du conteneur..."
docker run -d --rm \
  --name $CONTAINER_NAME \
  -p 8080:8080 \
  --env-file .env \
  -v $(pwd)/output:/app/output \
  $IMAGE_NAME

# Attente du démarrage
echo "⏳ Attente du démarrage..."
sleep 5

echo "🔍 Test endpoint racine /"
curl -v http://localhost:8080/ || true

echo "🔍 Test endpoint /health"
curl -v http://localhost:8080/health || true

echo "🔍 Test endpoint /cua/direct"
curl -s -X POST http://localhost:8080/cua/direct \
  -H "Content-Type: application/json" \
  -d '{
    "parcel": "AC 0494",
    "insee": "33234",
    "commune": "Latresne",
    "schema_whitelist": ["public"],
    "values_limit": 50,
    "carve_enclaves": true,
    "enclave_buffer_m": 200.0,
    "make_report": true,
    "make_map": true,
    "max_features_per_layer_on_map": 2000
  }' | jq .

echo "🧹 Arrêt du conteneur..."
docker stop $CONTAINER_NAME
