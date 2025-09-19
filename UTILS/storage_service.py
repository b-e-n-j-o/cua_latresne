# storage_service.py
import os
import logging
from pathlib import Path
from supabase import create_client, Client

from typing import Optional


log = logging.getLogger("cua.storage")

import dotenv

dotenv.load_dotenv()

# Client Supabase partagé
_SUPABASE_CLIENT = None
def get_supabase_client() -> Client:
    """Initialise et retourne le client Supabase."""
    global _SUPABASE_CLIENT
    if _SUPABASE_CLIENT is None:
        try:
            url = os.getenv("SUPABASE_URL")
            key = os.getenv("SUPABASE_SERVICE_KEY")
            if not url or not key:
                raise ValueError("SUPABASE_URL ou SUPABASE_KEY non définis")
            _SUPABASE_CLIENT = create_client(url, key)
            log.info("Client Supabase initialisé avec succès.")
        except Exception as e:
            log.error(f"Erreur d'initialisation du client Supabase: {e}")
            _SUPABASE_CLIENT = None
    return _SUPABASE_CLIENT

def upload_artifact(bucket_name: str, file_path: Path, job_id: str) -> Optional[str]:
    """
    Téléverse un fichier local dans un bucket Supabase avec Content-Type correct.
    """
    client = get_supabase_client()
    if not client:
        return None

    file_name_in_bucket = f"jobs/{job_id}/{file_path.name}"

    # --- Détection du type MIME ---
    import mimetypes
    ext = file_path.suffix.lower()

    manual_map = {
        ".html": "text/html",
        ".htm": "text/html",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".pdf": "application/pdf",
        ".json": "application/json",
        ".csv": "text/csv",
    }

    mime_type = manual_map.get(ext)
    if not mime_type:
        mime_type, _ = mimetypes.guess_type(file_path)
    if not mime_type:
        mime_type = "application/octet-stream"

    try:
        with open(file_path, 'rb') as f:
            data = f.read()

        # Upload avec Content-Type explicite
        res = client.storage.from_(bucket_name).upload(
            file_name_in_bucket,
            data,
            {"content-type": mime_type}
        )

        # URL publique
        public_url = client.storage.from_(bucket_name).get_public_url(file_name_in_bucket)
        log.info(f"✅ Fichier '{file_path.name}' téléversé ({mime_type}) → {public_url}")
        return public_url
    except Exception as e:
        log.error(f"❌ Échec upload '{file_path.name}': {e}")
        return None